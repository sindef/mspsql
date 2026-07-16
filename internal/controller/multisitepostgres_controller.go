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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

// MultiSitePostgresReconciler reconciles a MultiSitePostgres object
type MultiSitePostgresReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	SystemNamespace string
	Now             func() time.Time
}

// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=multisitepostgres,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=multisitepostgres/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=multisitepostgres/finalizers,verbs=update
// +kubebuilder:rbac:groups=multisite-postgres.multisite-postgres.dev,resources=siteregistrations,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch;create;update;patch;delete

func (r *MultiSitePostgresReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var instance multisitepostgresv1alpha1.MultiSitePostgres
	if err := r.Get(ctx, req.NamespacedName, &instance); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &instance)
	}
	if !controllerutil.ContainsFinalizer(&instance, instanceFinalizer) {
		controllerutil.AddFinalizer(&instance, instanceFinalizer)
		if err := r.Update(ctx, &instance); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	registrations := make(map[string]*multisitepostgresv1alpha1.SiteRegistration, len(instance.Spec.Sites))
	allConnected := true
	for _, site := range instance.Spec.Sites {
		var registration multisitepostgresv1alpha1.SiteRegistration
		if err := r.Get(ctx, client.ObjectKey{Name: site.SiteRegistrationRef}, &registration); err != nil {
			setCondition(&instance.Status.Conditions, instance.Generation, "SitesRegistered",
				metav1.ConditionFalse, "SiteNotFound", err.Error())
			instance.Status.Phase = "ValidatingSites"
			return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
		}
		if err := validateSitePolicy(site, &registration); err != nil {
			setCondition(&instance.Status.Conditions, instance.Generation, "StorageValidated",
				metav1.ConditionFalse, "SitePolicyRejected", fmt.Sprintf("%s: %v", site.Name, err))
			instance.Status.Phase = "ValidatingSites"
			return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
		}
		registrations[site.Name] = &registration
		allConnected = allConnected && registration.Status.Phase == "Connected"
	}
	setCondition(&instance.Status.Conditions, instance.Generation, "SitesRegistered",
		metav1.ConditionTrue, "AllSitesRegistered", "All referenced sites are registered")
	setCondition(&instance.Status.Conditions, instance.Generation, "StorageValidated",
		metav1.ConditionTrue, "SitePolicyAccepted", "StorageClasses, issuers and address pools are permitted")
	if !allConnected {
		setCondition(&instance.Status.Conditions, instance.Generation, "AgentsConnected",
			metav1.ConditionFalse, "AgentDisconnected", "One or more site agents are disconnected")
		instance.Status.Phase = "Pending"
		return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
	}
	setCondition(&instance.Status.Conditions, instance.Generation, "AgentsConnected",
		metav1.ConditionTrue, "AllAgentsConnected", "All site agents are connected")

	fingerprint, err := planFingerprint(&instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if instance.Status.PlanFingerprint != fingerprint {
		instance.Status.ActiveRevision++
		if instance.Status.ActiveRevision == 0 {
			instance.Status.ActiveRevision = 1
		}
		instance.Status.PlanFingerprint = fingerprint
	}
	systemNamespace := r.SystemNamespace
	if systemNamespace == "" {
		systemNamespace = "mspsql-system"
	}
	privateKey, err := ensureSigningKey(ctx, r.Client, systemNamespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	siteStatuses := make([]multisitepostgresv1alpha1.SiteRevisionStatus, 0, len(instance.Spec.Sites))
	memberAddresses := make(map[string]string)
	for _, siteStatus := range instance.Status.Sites {
		for member, address := range siteStatus.Addresses {
			memberAddresses[member] = address
		}
	}
	for _, site := range instance.Spec.Sites {
		registration := registrations[site.Name]
		desired := plan.SitePlan{
			SiteUID:         string(registration.UID),
			InstanceUID:     string(instance.UID),
			HubNamespace:    instance.Namespace,
			HubName:         instance.Name,
			Revision:        instance.Status.ActiveRevision,
			GeneratedAt:     now().UTC(),
			Site:            site,
			Postgres:        instance.Spec.Postgres,
			TDE:             instance.Spec.TDE,
			Backup:          instance.Spec.Backup,
			MemberAddresses: memberAddresses,
		}
		envelope, err := plan.Sign(privateKey, desired)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcilePlan(ctx, &instance, site.Name, string(registration.UID), envelope); err != nil {
			return ctrl.Result{}, err
		}
		status := previousSiteStatus(instance.Status.Sites, site.Name)
		status.Name = site.Name
		status.SiteRegistrationRef = site.SiteRegistrationRef
		status.DesiredRevision = instance.Status.ActiveRevision
		if status.Phase == "" {
			status.Phase = "PlanIssued"
		}
		siteStatuses = append(siteStatuses, status)
	}
	instance.Status.Sites = siteStatuses
	instance.Status.ObservedGeneration = instance.Generation
	instance.Status.Phase = "Reconciling"
	setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
		metav1.ConditionFalse, "PlansIssued", "Waiting for all sites to apply the active revision")
	if allSitesApplied(instance.Status.Sites, instance.Status.ActiveRevision) {
		instance.Status.Phase = "Ready"
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionTrue, "AllSitesReady", "All sites applied the active revision")
	}
	if err := r.updateInstanceStatus(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MultiSitePostgresReconciler) reconcilePlan(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres, siteName, siteUID string, envelope plan.Envelope,
) error {
	data, err := envelopeData(envelope)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("mspsql-plan-%s-%s", instance.Name, siteName)
	var configMap corev1.ConfigMap
	key := client.ObjectKey{Namespace: instance.Namespace, Name: name}
	err = r.Get(ctx, key, &configMap)
	if apierrors.IsNotFound(err) {
		configMap = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: instance.Namespace,
				Name:      name,
				Labels: map[string]string{
					"multisite-postgres.dev/instance-uid":          string(instance.UID),
					"multisite-postgres.dev/site-name":             siteName,
					"multisite-postgres.dev/site-registration-uid": siteUID,
				},
			},
			Data: data,
		}
		if err := controllerutil.SetControllerReference(instance, &configMap, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, &configMap)
	}
	if err != nil {
		return err
	}
	if configMap.Data["envelope.json"] == data["envelope.json"] {
		return nil
	}
	configMap.Data = data
	return r.Update(ctx, &configMap)
}

func (r *MultiSitePostgresReconciler) finalize(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(instance, instanceFinalizer) {
		return ctrl.Result{}, nil
	}
	if instance.Annotations["multisite-postgres.dev/force-orphan"] == "true" {
		controllerutil.RemoveFinalizer(instance, instanceFinalizer)
		return ctrl.Result{}, r.Update(ctx, instance)
	}
	var waiting []string
	for _, site := range instance.Status.Sites {
		if site.Phase != "Deleted" {
			waiting = append(waiting, site.Name)
		}
	}
	if len(waiting) > 0 {
		instance.Status.Phase = "Deleting"
		setCondition(&instance.Status.Conditions, instance.Generation, "DeletionBlocked",
			metav1.ConditionTrue, "AwaitingSites", "Waiting for deletion acknowledgement from: "+
				strings.Join(waiting, ", "))
		if err := r.updateInstanceStatus(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	controllerutil.RemoveFinalizer(instance, instanceFinalizer)
	return ctrl.Result{}, r.Update(ctx, instance)
}

func (r *MultiSitePostgresReconciler) updateInstanceStatus(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) error {
	return r.Status().Update(ctx, instance)
}

func previousSiteStatus(statuses []multisitepostgresv1alpha1.SiteRevisionStatus,
	name string,
) multisitepostgresv1alpha1.SiteRevisionStatus {
	for _, status := range statuses {
		if status.Name == name {
			return status
		}
	}
	return multisitepostgresv1alpha1.SiteRevisionStatus{}
}

func allSitesApplied(statuses []multisitepostgresv1alpha1.SiteRevisionStatus, revision int64) bool {
	if len(statuses) == 0 {
		return false
	}
	for _, status := range statuses {
		if status.AppliedRevision != revision {
			return false
		}
	}
	return true
}

// SetupWithManager sets up the controller with the Manager.
func (r *MultiSitePostgresReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.MultiSitePostgres{}).
		Owns(&corev1.ConfigMap{}).
		WithEventFilter(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
			predicate.NewPredicateFuncs(func(object client.Object) bool {
				return !object.GetDeletionTimestamp().IsZero()
			}),
		)).
		Named("multisitepostgres").
		Complete(r)
}

func planFingerprint(instance *multisitepostgresv1alpha1.MultiSitePostgres) (string, error) {
	payload, err := json.Marshal(struct {
		Generation int64
		Addresses  map[string]map[string]string
	}{
		Generation: instance.Generation,
		Addresses: func() map[string]map[string]string {
			addresses := make(map[string]map[string]string, len(instance.Status.Sites))
			for _, site := range instance.Status.Sites {
				addresses[site.Name] = site.Addresses
			}
			return addresses
		}(),
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
