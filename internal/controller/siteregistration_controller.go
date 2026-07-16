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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/registration"
	"github.com/sindef/mspsql/internal/wireguard"
)

// SiteRegistrationReconciler reconciles a SiteRegistration object
type SiteRegistrationReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	SystemNamespace       string
	RegistrationPublicURL string
	WireGuardNetworkCIDR  string
	Now                   func() time.Time
}

const siteHeartbeatTimeout = 150 * time.Second

// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=siteregistrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=siteregistrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=siteregistrations/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *SiteRegistrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var site multisitepostgresv1alpha1.SiteRegistration
	if err := r.Get(ctx, req.NamespacedName, &site); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !site.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	systemNamespace := r.SystemNamespace
	if systemNamespace == "" {
		systemNamespace = "mspsql-system"
	}
	if site.Spec.Revoked {
		return ctrl.Result{}, r.reconcileRevoked(ctx, &site, systemNamespace)
	}
	if _, err := wireguard.EnsureHubIdentity(ctx, r.Client, systemNamespace,
		r.WireGuardNetworkCIDR); err != nil {
		return ctrl.Result{}, err
	}
	if err := wireguard.RenderPeers(ctx, r.Client, systemNamespace); err != nil {
		return ctrl.Result{}, err
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	if _, err := ensureSigningKey(ctx, r.Client, systemNamespace); err != nil {
		return ctrl.Result{}, err
	}
	secretKey := types.NamespacedName{
		Namespace: systemNamespace,
		Name:      "registration-" + string(site.UID),
	}
	var secret corev1.Secret
	err := r.Get(ctx, secretKey, &secret)
	if apierrors.IsNotFound(err) || tokenExpired(&secret, now()) {
		token, tokenErr := registration.NewToken(now())
		if tokenErr != nil {
			return ctrl.Result{}, tokenErr
		}
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: secretKey.Namespace, Name: secretKey.Name},
			Immutable:  ptr(true),
			Data: map[string][]byte{
				"sha256":    token.Hash,
				"expiresAt": []byte(token.ExpiresAt.UTC().Format(time.RFC3339Nano)),
			},
		}
		if err == nil {
			if deleteErr := r.Delete(ctx, &secret); deleteErr != nil {
				return ctrl.Result{}, deleteErr
			}
			secret.ResourceVersion = ""
			secret.UID = ""
		}
		if err := controllerutil.SetControllerReference(&site, &secret, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, &secret); err != nil {
			return ctrl.Result{}, err
		}
		baseURL := r.RegistrationPublicURL
		if baseURL == "" {
			baseURL = "https://registration.invalid"
		}
		site.Status.RegistrationURL = fmt.Sprintf("%s/%s/registration.yaml", baseURL, token.Value)
		expiresAt := metav1.NewTime(token.ExpiresAt)
		site.Status.RegistrationExpiresAt = &expiresAt
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	requeueAfter := time.Duration(0)
	site.Status.Phase = "Pending"
	if site.Status.ClusterUID != "" {
		site.Status.Phase = "Registered"
		if site.Status.LastHeartbeatTime != nil &&
			now().Sub(site.Status.LastHeartbeatTime.Time) <= siteHeartbeatTimeout {
			site.Status.Phase = "Connected"
			requeueAfter = siteHeartbeatTimeout - now().Sub(site.Status.LastHeartbeatTime.Time)
			setCondition(&site.Status.Conditions, site.Generation, "Connected", metav1.ConditionTrue,
				"HeartbeatCurrent", "The authenticated site agent heartbeat is current")
		} else {
			setCondition(&site.Status.Conditions, site.Generation, "Connected", metav1.ConditionFalse,
				"HeartbeatStale", "No authenticated site agent heartbeat is current")
		}
	}
	setCondition(&site.Status.Conditions, site.Generation, "Registered", metav1.ConditionFalse,
		"AwaitingAgent", "Waiting for the site agent to bind this registration")
	if site.Status.ClusterUID != "" {
		setCondition(&site.Status.Conditions, site.Generation, "Registered", metav1.ConditionTrue,
			"ClusterBound", "Registration is bound to an immutable Kubernetes cluster UID")
		switch {
		case site.Status.AgentCertificateExpiresAt == nil:
			setCondition(&site.Status.Conditions, site.Generation, "IdentityReady", metav1.ConditionFalse,
				"CertificateExpiryUnknown", "The agent has not reported its certificate expiry")
		case !now().Before(site.Status.AgentCertificateExpiresAt.Time):
			setCondition(&site.Status.Conditions, site.Generation, "IdentityReady", metav1.ConditionFalse,
				"CertificateExpired", "The agent mTLS certificate has expired")
		case now().Add(2 * time.Hour).After(site.Status.AgentCertificateExpiresAt.Time):
			setCondition(&site.Status.Conditions, site.Generation, "IdentityReady", metav1.ConditionFalse,
				"CertificateExpiring", "The agent mTLS certificate is approaching expiry")
			requeueAfter = minPositive(requeueAfter,
				site.Status.AgentCertificateExpiresAt.Sub(now()))
		default:
			setCondition(&site.Status.Conditions, site.Generation, "IdentityReady", metav1.ConditionTrue,
				"CertificateCurrent", "The agent mTLS certificate is current")
			requeueAfter = minPositive(requeueAfter,
				site.Status.AgentCertificateExpiresAt.Add(-2*time.Hour).Sub(now()))
		}
	}
	var peer corev1.Secret
	peerErr := r.Get(ctx, types.NamespacedName{
		Namespace: systemNamespace, Name: "wireguard-peer-" + string(site.UID),
	}, &peer)
	switch {
	case peerErr == nil:
		setCondition(&site.Status.Conditions, site.Generation, "WireGuardReady", metav1.ConditionTrue,
			"PeerAuthorized", "The site WireGuard peer is present in the gateway configuration")
	case apierrors.IsNotFound(peerErr):
		setCondition(&site.Status.Conditions, site.Generation, "WireGuardReady", metav1.ConditionFalse,
			"AwaitingPeer", "Waiting for the site agent to authorize its WireGuard key")
	default:
		return ctrl.Result{}, peerErr
	}
	if err := r.Status().Update(ctx, &site); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *SiteRegistrationReconciler) reconcileRevoked(ctx context.Context,
	site *multisitepostgresv1alpha1.SiteRegistration, systemNamespace string,
) error {
	for _, name := range []string{"registration-" + string(site.UID)} {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: systemNamespace, Name: name}}
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if err := wireguard.RevokePeer(ctx, r.Client, systemNamespace, site.UID); err != nil {
		return err
	}
	site.Status.Phase = "Revoked"
	site.Status.RegistrationURL = ""
	site.Status.RegistrationExpiresAt = nil
	setCondition(&site.Status.Conditions, site.Generation, "Registered", metav1.ConditionFalse,
		"AdministrativelyRevoked", "The site identity has been revoked")
	setCondition(&site.Status.Conditions, site.Generation, "Connected", metav1.ConditionFalse,
		"AdministrativelyRevoked", "Control connections from this site are rejected")
	setCondition(&site.Status.Conditions, site.Generation, "IdentityReady", metav1.ConditionFalse,
		"AdministrativelyRevoked", "The site identity has been revoked")
	setCondition(&site.Status.Conditions, site.Generation, "WireGuardReady", metav1.ConditionFalse,
		"AdministrativelyRevoked", "The WireGuard peer has been removed")
	return r.Status().Update(ctx, site)
}

func tokenExpired(secret *corev1.Secret, now time.Time) bool {
	encoded := secret.Data["expiresAt"]
	if len(secret.Data["sha256"]) == 0 || len(encoded) == 0 {
		return true
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, string(encoded))
	return err != nil || !now.Before(expiresAt)
}

func minPositive(current, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current == 0 || candidate < current {
		return candidate
	}
	return current
}

func ptr[T any](value T) *T {
	return &value
}

// SetupWithManager sets up the controller with the Manager.
func (r *SiteRegistrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.SiteRegistration{}).
		Named("siteregistration").
		Complete(r)
}
