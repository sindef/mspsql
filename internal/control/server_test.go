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

package control

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestAggregateConditionsExcludesWitnessFromPatroni(t *testing.T) {
	healthy := func(conditionTypes ...string) []metav1.Condition {
		conditions := make([]metav1.Condition, 0, len(conditionTypes))
		for _, conditionType := range conditionTypes {
			conditions = append(conditions, metav1.Condition{
				Type: conditionType, Status: metav1.ConditionTrue, Reason: "Healthy",
			})
		}
		return conditions
	}
	instance := &api.MultiSitePostgres{
		ObjectMeta: metav1.ObjectMeta{Generation: 4},
		Spec: api.MultiSitePostgresSpec{Sites: []api.PostgresSiteSpec{
			{Name: "vic", Role: api.SiteRoleData},
			{Name: "nsw", Role: api.SiteRoleWitness},
		}},
		Status: api.MultiSitePostgresStatus{Sites: []api.SiteRevisionStatus{
			{Name: "vic", Conditions: healthy(
				"LoadBalancersAllocated", "CertificatesReady", "EtcdQuorate", "PatroniReady",
			)},
			{Name: "nsw", Conditions: healthy(
				"LoadBalancersAllocated", "CertificatesReady", "EtcdQuorate",
			)},
		}},
	}
	aggregateInstanceConditions(instance)
	patroni := meta.FindStatusCondition(instance.Status.Conditions, "PatroniReady")
	if patroni == nil || patroni.Status != metav1.ConditionTrue || patroni.ObservedGeneration != 4 {
		t.Fatalf("PatroniReady = %#v", patroni)
	}
}
