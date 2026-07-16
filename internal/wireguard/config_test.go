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

package wireguard

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestAuthorizeAndRevokePeers(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	sites := []*api.SiteRegistration{
		{ObjectMeta: metav1.ObjectMeta{Name: "vic", UID: types.UID("site-vic")}},
		{ObjectMeta: metav1.ObjectMeta{Name: "nsw", UID: types.UID("site-nsw")}},
	}
	configurations := make([]string, 0, len(sites))
	for _, site := range sites {
		configuration, err := AuthorizePeer(context.Background(), kube, "system",
			"10.254.0.0/29", "wireguard.example:51820", site,
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
		if err != nil {
			t.Fatal(err)
		}
		configurations = append(configurations, configuration)
	}
	if configurations[0] == configurations[1] ||
		!strings.Contains(configurations[0], "AllowedIPs = 10.254.0.1/32") {
		t.Fatalf("peer configurations = %#v", configurations)
	}
	var peers corev1.ConfigMap
	if err := kube.Get(context.Background(), client.ObjectKey{
		Namespace: "system", Name: PeersConfigMapName,
	}, &peers); err != nil {
		t.Fatal(err)
	}
	if strings.Count(peers.Data["peers.conf"], "[Peer]") != 2 {
		t.Fatalf("rendered peers:\n%s", peers.Data["peers.conf"])
	}
	if err := RevokePeer(context.Background(), kube, "system", sites[0].UID); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKey{
		Namespace: "system", Name: PeersConfigMapName,
	}, &peers); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(peers.Data["peers.conf"], "vic") ||
		strings.Count(peers.Data["peers.conf"], "[Peer]") != 1 {
		t.Fatalf("peers after revocation:\n%s", peers.Data["peers.conf"])
	}
}

func TestNetworkContractRejectsUnsafeRanges(t *testing.T) {
	for _, value := range []string{"", "10.254.0.1/29", "10.254.0.0/30", "fd00::/64"} {
		if value == "" {
			continue
		}
		if _, err := parseNetwork(value); err == nil {
			t.Fatalf("network %q was accepted", value)
		}
	}
}
