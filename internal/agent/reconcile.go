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
	"maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sindef/mspsql/internal/plan"
)

type ApplyResult struct {
	Phase      string
	Addresses  map[string]string
	Conditions []metav1.Condition
}

type Reconciler struct {
	Client    client.Client
	Renderer  Renderer
	HubDomain string
	SiteUID   string
}

func (r *Reconciler) Apply(ctx context.Context, desired, previous plan.SitePlan,
	connected bool,
) (ApplyResult, error) {
	result := ApplyResult{Phase: "CreatingNamespaces", Addresses: map[string]string{}}
	if err := EnsureNamespace(ctx, r.Client, desired.Site.Namespace, r.HubDomain, r.SiteUID,
		desired.InstanceUID, connected); err != nil {
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse,
			"NamespaceOwnershipConflict", err.Error())
		return result, err
	}

	result.Phase = "AllocatingLoadBalancers"
	for _, object := range r.Renderer.LoadBalancers(desired) {
		if !connected {
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
				if apierrors.IsNotFound(err) {
					setLocalCondition(&result.Conditions, "AddressChangeBlocked", metav1.ConditionTrue,
						"HubDisconnected", "LoadBalancer Services are not recreated while disconnected")
				}
				return result, err
			}
		} else if err := r.apply(ctx, object); err != nil {
			return result, err
		}
		var service corev1.Service
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(object), &service); err != nil {
			return result, err
		}
		address, err := loadBalancerAddress(&service)
		if err != nil {
			setLocalCondition(&result.Conditions, "LoadBalancersAllocated", metav1.ConditionFalse,
				"AddressPending", err.Error())
			return result, nil
		}
		result.Addresses[service.Name] = address
	}
	setLocalCondition(&result.Conditions, "LoadBalancersAllocated", metav1.ConditionTrue,
		"AddressesAllocated", "All member addresses are allocated")

	if !connected && plan.Classify(previous, desired) == plan.MutationCoordinated {
		result.Phase = "WaitingForHub"
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse,
			"HubRequired", "The desired revision contains globally coordinated changes")
		return result, nil
	}
	if connected {
		desired.MemberAddresses = mergeAddresses(desired.MemberAddresses, result.Addresses)
		result.Phase = "IssuingCertificates"
		for _, object := range r.Renderer.Certificates(desired) {
			if err := r.apply(ctx, object); err != nil {
				return result, err
			}
		}
	}

	result.Phase = "ReconcilingWorkloads"
	desired.MemberAddresses = mergeAddresses(desired.MemberAddresses, result.Addresses)
	objects, err := r.Renderer.Workloads(desired)
	if err != nil {
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse, "GlobalAddressesPending", err.Error())
		return result, nil
	}
	for _, object := range objects {
		if err := r.apply(ctx, object); err != nil {
			return result, err
		}
	}
	result.Phase = "Ready"
	setLocalCondition(&result.Conditions, "Ready", metav1.ConditionTrue,
		"DesiredStateApplied", "All locally managed resources match the signed plan")
	return result, nil
}

func (r *Reconciler) apply(ctx context.Context, object client.Object) error {
	encoded, err := json.Marshal(object)
	if err != nil {
		return err
	}
	return r.Client.Patch(ctx, object, client.RawPatch(types.ApplyPatchType, encoded),
		client.FieldOwner("mspsql-agent"))
}

func loadBalancerAddress(service *corev1.Service) (string, error) {
	if len(service.Status.LoadBalancer.Ingress) != 1 {
		return "", fmt.Errorf("service %s must have exactly one ingress address", service.Name)
	}
	ingress := service.Status.LoadBalancer.Ingress[0]
	if ingress.IP != "" {
		return ingress.IP, nil
	}
	if ingress.Hostname != "" {
		return ingress.Hostname, nil
	}
	return "", fmt.Errorf("service %s ingress address is empty", service.Name)
}

func mergeAddresses(first, second map[string]string) map[string]string {
	merged := make(map[string]string, len(first)+len(second))
	maps.Copy(merged, first)
	maps.Copy(merged, second)
	return merged
}

func setLocalCondition(conditions *[]metav1.Condition, conditionType string,
	status metav1.ConditionStatus, reason, message string,
) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: conditionType, Status: status, Reason: reason, Message: message,
	})
}
