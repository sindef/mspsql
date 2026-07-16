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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
		ready, message, err := r.certificatesReady(ctx, r.Renderer.Certificates(desired))
		if err != nil {
			return result, err
		}
		if !ready {
			setLocalCondition(&result.Conditions, "CertificatesReady", metav1.ConditionFalse,
				"IssuancePending", message)
			return result, nil
		}
		setLocalCondition(&result.Conditions, "CertificatesReady", metav1.ConditionTrue,
			"CertificatesIssued", "All workload certificates are Ready")
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
	ready, message, err := r.workloadsReady(ctx, objects)
	if err != nil {
		return result, err
	}
	if !ready {
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse,
			"WorkloadsProgressing", message)
		return result, nil
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

func (r *Reconciler) certificatesReady(ctx context.Context, certificates []client.Object) (bool, string, error) {
	for _, expected := range certificates {
		certificate, ok := expected.(*unstructured.Unstructured)
		if !ok {
			return false, "", fmt.Errorf("certificate renderer returned %T", expected)
		}
		observed := &unstructured.Unstructured{}
		observed.SetGroupVersionKind(certificate.GroupVersionKind())
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(certificate), observed); err != nil {
			return false, "", err
		}
		conditions, found, err := unstructured.NestedSlice(observed.Object, "status", "conditions")
		if err != nil {
			return false, "", err
		}
		isReady := false
		for _, raw := range conditions {
			condition, conditionOK := raw.(map[string]any)
			if conditionOK && condition["type"] == "Ready" && condition["status"] == "True" {
				isReady = true
				break
			}
		}
		if isReady {
			continue
		}
		if !found {
			return false, fmt.Sprintf("Certificate %s has not reported status", observed.GetName()), nil
		}
		return false, fmt.Sprintf("Certificate %s is not Ready", observed.GetName()), nil
	}
	return true, "", nil
}

func (r *Reconciler) workloadsReady(ctx context.Context, objects []client.Object) (bool, string, error) {
	for _, expected := range objects {
		switch expected := expected.(type) {
		case *appsv1.StatefulSet:
			var observed appsv1.StatefulSet
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(expected), &observed); err != nil {
				return false, "", err
			}
			replicas := int32(1)
			if expected.Spec.Replicas != nil {
				replicas = *expected.Spec.Replicas
			}
			if observed.Status.ObservedGeneration < observed.Generation ||
				observed.Status.ReadyReplicas != replicas {
				return false, fmt.Sprintf("StatefulSet %s has %d/%d Ready replicas",
					observed.Name, observed.Status.ReadyReplicas, replicas), nil
			}
		case *appsv1.Deployment:
			var observed appsv1.Deployment
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(expected), &observed); err != nil {
				return false, "", err
			}
			replicas := int32(1)
			if expected.Spec.Replicas != nil {
				replicas = *expected.Spec.Replicas
			}
			if observed.Status.ObservedGeneration < observed.Generation ||
				observed.Status.AvailableReplicas != replicas ||
				observed.Status.UpdatedReplicas != replicas {
				return false, fmt.Sprintf("Deployment %s has %d/%d Available replicas",
					observed.Name, observed.Status.AvailableReplicas, replicas), nil
			}
		}
	}
	return true, "", nil
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
