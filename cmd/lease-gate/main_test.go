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

package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSetActiveLabelControlsServiceMembership(t *testing.T) {
	kube := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "system", Name: "gateway", Labels: map[string]string{"app": "gateway"},
	}})
	for _, active := range []bool{true, false} {
		if err := setActiveLabel(context.Background(), kube, "system", "gateway", active); err != nil {
			t.Fatal(err)
		}
		pod, err := kube.CoreV1().Pods("system").Get(context.Background(), "gateway", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, found := pod.Labels["multisite-postgres.dev/gateway-active"]
		if found != active {
			t.Fatalf("active label presence = %t, want %t", found, active)
		}
	}
}
