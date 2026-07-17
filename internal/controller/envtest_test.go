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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

var _ = Describe("API-server reconciliation", Ordered, func() {
	const systemNamespace = "envtest-system"
	var siteUID types.UID

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: systemNamespace},
		})).To(Succeed())
	})

	It("rejects a SiteRegistration missing required policy fields", func() {
		invalid := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": api.GroupVersion.String(),
			"kind":       "SiteRegistration",
			"metadata":   map[string]any{"name": "invalid-site"},
			"spec":       map[string]any{},
		}}
		invalid.SetGroupVersionKind(schema.GroupVersionKind{
			Group: api.GroupVersion.Group, Version: api.GroupVersion.Version, Kind: "SiteRegistration",
		})
		err := k8sClient.Create(ctx, invalid)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "error was %v", err)
	})

	It("creates registration state and recovers a deleted child", func() {
		site := &api.SiteRegistration{
			ObjectMeta: metav1.ObjectMeta{Name: "envtest-site"},
			Spec: api.SiteRegistrationSpec{
				PermittedStorageClasses: api.StorageClassPolicy{},
				PermittedIssuers:        api.IssuerPolicy{},
			},
		}
		Expect(k8sClient.Create(ctx, site)).To(Succeed())
		siteUID = site.UID
		reconciler := SiteRegistrationReconciler{
			Client: k8sClient, Scheme: clientgoscheme.Scheme, SystemNamespace: systemNamespace,
			RegistrationPublicURL: "https://registration.example",
			WireGuardNetworkCIDR:  "10.254.0.0/16",
		}
		request := ctrl.Request{NamespacedName: client.ObjectKey{Name: site.Name}}
		_, err := reconciler.Reconcile(context.Background(), request)
		Expect(err).NotTo(HaveOccurred())

		key := client.ObjectKey{Namespace: systemNamespace, Name: "registration-" + string(siteUID)}
		var token corev1.Secret
		Expect(k8sClient.Get(ctx, key, &token)).To(Succeed())
		Expect(token.Immutable).NotTo(BeNil())
		Expect(*token.Immutable).To(BeTrue())
		Expect(token.OwnerReferences).To(HaveLen(1))
		Expect(token.OwnerReferences[0].UID).To(Equal(siteUID))

		var reconciled api.SiteRegistration
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: site.Name}, &reconciled)).To(Succeed())
		Expect(reconciled.Status.ObservedGeneration).To(Equal(reconciled.Generation))
		Expect(reconciled.Status.Phase).To(Equal("Pending"))
		Expect(reconciled.Status.RegistrationURL).To(HavePrefix("https://registration.example/"))

		Expect(k8sClient.Delete(ctx, &token)).To(Succeed())
		_, err = reconciler.Reconcile(context.Background(), request)
		Expect(err).NotTo(HaveOccurred())
		var recreated corev1.Secret
		Expect(k8sClient.Get(ctx, key, &recreated)).To(Succeed())
		Expect(recreated.UID).NotTo(Equal(token.UID))
	})

	AfterAll(func() {
		for _, object := range []client.Object{
			&api.SiteRegistration{ObjectMeta: metav1.ObjectMeta{Name: "envtest-site"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: systemNamespace}},
		} {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, object))).To(Succeed())
		}
	})
})
