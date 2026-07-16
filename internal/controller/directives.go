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

package controller

import (
	"context"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func reconcileDirective(ctx context.Context, kube client.Client, scheme *runtime.Scheme, owner client.Object,
	name, directiveType, instanceRef string, spec any, deleting bool,
) error {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	key := client.ObjectKey{Namespace: owner.GetNamespace(), Name: name}
	data := map[string]string{
		"type":        directiveType,
		"instanceRef": instanceRef,
		"deleting":    boolString(deleting),
		"spec.json":   string(encoded),
	}
	var configMap corev1.ConfigMap
	if err := kube.Get(ctx, key, &configMap); apierrors.IsNotFound(err) {
		configMap = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: owner.GetNamespace(),
				Name:      name,
				Labels: map[string]string{
					"multisite-postgres.dev/directive":    directiveType,
					"multisite-postgres.dev/instance-ref": instanceRef,
				},
			},
			Data: data,
		}
		if err := controllerutil.SetControllerReference(owner, &configMap, scheme); err != nil {
			return err
		}
		return kube.Create(ctx, &configMap)
	} else if err != nil {
		return err
	}
	if mapsEqual(configMap.Data, data) {
		return nil
	}
	configMap.Data = data
	return kube.Update(ctx, &configMap)
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
