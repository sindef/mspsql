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
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
)

// EventReporter emits target-cluster Events only when locally observed state changes.
type EventReporter struct {
	Recorder  events.EventRecorder
	Namespace string

	mu       sync.Mutex
	observed map[string]map[string]string
}

func (r *EventReporter) Observe(instanceUID string, result ApplyResult, reconcileErr error) {
	if r == nil || r.Recorder == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.observed == nil {
		r.observed = map[string]map[string]string{}
	}
	current := r.observed[instanceUID]
	if current == nil {
		current = map[string]string{}
		r.observed[instanceUID] = current
	}
	regarding := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: r.Namespace, Name: cacheName(instanceUID),
	}}
	for _, condition := range result.Conditions {
		signature := string(condition.Status) + "/" + condition.Reason
		key := "condition/" + condition.Type
		if current[key] == signature {
			continue
		}
		current[key] = signature
		eventType := corev1.EventTypeNormal
		if condition.Status == metav1.ConditionFalse {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Eventf(regarding, nil, eventType, condition.Reason,
			"ConditionChanged", "%s: %s", condition.Type, condition.Message)
	}
	errorSignature := ""
	if reconcileErr != nil {
		errorSignature = reconcileErr.Error()
	}
	if current["reconcile/error"] != errorSignature {
		current["reconcile/error"] = errorSignature
		if reconcileErr != nil {
			r.Recorder.Eventf(regarding, nil, corev1.EventTypeWarning, "ReconcileFailed",
				"Reconcile", "Revision reconciliation failed: %s", reconcileErr)
		} else {
			r.Recorder.Eventf(regarding, nil, corev1.EventTypeNormal, "ReconcileRecovered",
				"Reconcile", "Revision reconciliation recovered in phase %s", result.Phase)
		}
	}
}
