/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

type SiteInventory struct {
	StorageClasses        []api.StorageClassInventory        `json:"storageClasses"`
	VolumeSnapshotClasses []api.VolumeSnapshotClassInventory `json:"volumeSnapshotClasses"`
	Issuers               []api.IssuerReference              `json:"issuers"`
}

func DiscoverInventory(ctx context.Context, kube client.Client) ([]byte, error) {
	var storageClasses storagev1.StorageClassList
	if err := kube.List(ctx, &storageClasses); err != nil {
		return nil, fmt.Errorf("list StorageClasses: %w", err)
	}
	inventory := SiteInventory{
		StorageClasses: make([]api.StorageClassInventory, 0, len(storageClasses.Items)),
	}
	for i := range storageClasses.Items {
		storageClass := &storageClasses.Items[i]
		discovered := api.StorageClassInventory{
			Name:                 storageClass.Name,
			Provisioner:          storageClass.Provisioner,
			AllowVolumeExpansion: storageClass.AllowVolumeExpansion != nil && *storageClass.AllowVolumeExpansion,
			AllowedTopologies:    topologyTerms(storageClass.AllowedTopologies),
		}
		if storageClass.ReclaimPolicy != nil {
			discovered.ReclaimPolicy = string(*storageClass.ReclaimPolicy)
		}
		if storageClass.VolumeBindingMode != nil {
			discovered.VolumeBindingMode = string(*storageClass.VolumeBindingMode)
		}
		inventory.StorageClasses = append(inventory.StorageClasses, discovered)
	}
	slices.SortFunc(inventory.StorageClasses, func(left, right api.StorageClassInventory) int {
		return strings.Compare(left.Name, right.Name)
	})
	issuers, err := discoverIssuers(ctx, kube)
	if err != nil {
		return nil, err
	}
	snapshotClasses, err := discoverVolumeSnapshotClasses(ctx, kube)
	if err != nil {
		return nil, err
	}
	inventory.Issuers = issuers
	inventory.VolumeSnapshotClasses = snapshotClasses
	return json.Marshal(inventory)
}

func topologyTerms(terms []corev1.TopologySelectorTerm) []string {
	var values []string
	for _, term := range terms {
		for _, expression := range term.MatchLabelExpressions {
			for _, value := range expression.Values {
				values = append(values, expression.Key+"="+value)
			}
		}
	}
	slices.Sort(values)
	return slices.Compact(values)
}

func discoverIssuers(ctx context.Context, kube client.Client) ([]api.IssuerReference, error) {
	var issuers []api.IssuerReference
	for _, kind := range []string{"Issuer", "ClusterIssuer"} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "cert-manager.io", Version: "v1", Kind: kind + "List",
		})
		if err := kube.List(ctx, list); err != nil {
			if meta.IsNoMatchError(err) {
				continue
			}
			return nil, fmt.Errorf("list cert-manager %s resources: %w", kind, err)
		}
		for i := range list.Items {
			issuers = append(issuers, api.IssuerReference{
				Name: list.Items[i].GetName(), Kind: kind, Group: "cert-manager.io",
			})
		}
	}
	slices.SortFunc(issuers, func(left, right api.IssuerReference) int {
		if comparison := strings.Compare(left.Kind, right.Kind); comparison != 0 {
			return comparison
		}
		return strings.Compare(left.Name, right.Name)
	})
	return slices.CompactFunc(issuers, func(left, right api.IssuerReference) bool {
		return left == right
	}), nil
}

func discoverVolumeSnapshotClasses(ctx context.Context,
	kube client.Client,
) ([]api.VolumeSnapshotClassInventory, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshotClassList",
	})
	if err := kube.List(ctx, list); err != nil {
		if meta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list VolumeSnapshotClasses: %w", err)
	}
	classes := make([]api.VolumeSnapshotClassInventory, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		driver, _, _ := unstructured.NestedString(item.Object, "driver")
		deletionPolicy, _, _ := unstructured.NestedString(item.Object, "deletionPolicy")
		classes = append(classes, api.VolumeSnapshotClassInventory{
			Name: item.GetName(), Driver: driver, DeletionPolicy: deletionPolicy,
		})
	}
	slices.SortFunc(classes, func(left, right api.VolumeSnapshotClassInventory) int {
		return strings.Compare(left.Name, right.Name)
	})
	return classes, nil
}
