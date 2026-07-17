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
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
)

func TestEventReporterEmitsOnlyTransitions(t *testing.T) {
	recorder := events.NewFakeRecorder(8)
	reporter := &EventReporter{Recorder: recorder, Namespace: "mspsql-agent"}
	result := ApplyResult{Phase: "Waiting", Conditions: []metav1.Condition{{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "DependencyPending",
		Message: "Waiting for a dependency",
	}}}
	reporter.Observe("instance", result, errors.New("dependency unavailable"))
	reporter.Observe("instance", result, errors.New("dependency unavailable"))
	result.Phase = "Ready"
	result.Conditions[0] = metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "DesiredStateApplied",
		Message: "All resources match",
	}
	reporter.Observe("instance", result, nil)

	var recorded []string
	for len(recorder.Events) > 0 {
		recorded = append(recorded, <-recorder.Events)
	}
	if len(recorded) != 4 {
		t.Fatalf("events = %#v", recorded)
	}
	joined := strings.Join(recorded, "\n")
	for _, expected := range []string{
		"Warning DependencyPending Ready: Waiting for a dependency",
		"Warning ReconcileFailed Revision reconciliation failed: dependency unavailable",
		"Normal DesiredStateApplied Ready: All resources match",
		"Normal ReconcileRecovered Revision reconciliation recovered in phase Ready",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("events missing %q: %s", expected, joined)
		}
	}
}

func TestEventNotesRespectAPILimit(t *testing.T) {
	note := boundedEventNote("failure: %s", strings.Repeat("界", 1024))
	if len(note) > 1024 || !utf8.ValidString(note) || !strings.HasSuffix(note, "...") {
		t.Fatalf("bounded note has %d bytes and valid=%t: %q", len(note), utf8.ValidString(note), note)
	}
}
