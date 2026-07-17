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
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

// MultiSitePostgresReconciler reconciles a MultiSitePostgres object
type MultiSitePostgresReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	SystemNamespace       string
	DefaultBackupSchedule string
	Now                   func() time.Time
}

// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=multisitepostgres,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=multisitepostgres/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=multisitepostgres/finalizers,verbs=update
// +kubebuilder:rbac:groups=multisite-postgres.dev,resources=siteregistrations,verbs=get;list;watch
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
	instance.Status.ObservedGeneration = instance.Generation

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
	if err := r.validateInstanceClaims(ctx, &instance); err != nil {
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "InstanceIsolationConflict", err.Error())
		instance.Status.Phase = "ValidatingSites"
		return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
	}
	if !allConnected {
		setCondition(&instance.Status.Conditions, instance.Generation, "AgentsConnected",
			metav1.ConditionFalse, "AgentDisconnected", "One or more site agents are disconnected")
		instance.Status.Primary = ""
		instance.Status.SynchronousStandbys = nil
		setCondition(&instance.Status.Conditions, instance.Generation, "TopologyReady",
			metav1.ConditionFalse, "AgentDisconnected", "Topology requires every data-site observer")
		instance.Status.Phase = "Pending"
		return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
	}
	setCondition(&instance.Status.Conditions, instance.Generation, "AgentsConnected",
		metav1.ConditionTrue, "AllAgentsConnected", "All site agents are connected")

	restorePlan, err := r.restorePlan(ctx, &instance)
	if err != nil {
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "RestoreContractInvalid", err.Error())
		instance.Status.Phase = "Restoring"
		return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
	}
	upgradePlan, majorUpgradePlan, err := r.upgradePlans(ctx, &instance)
	if err != nil {
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "UpgradeContractInvalid", err.Error())
		instance.Status.Phase = "Upgrading"
		return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
	}
	memberAddresses, addressCandidates, addressMigration, err := r.addressPlan(ctx, &instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	credentialRotation, err := r.credentialRotationPlan(ctx, &instance,
		addressMigration, restorePlan, upgradePlan, majorUpgradePlan)
	if err != nil {
		return ctrl.Result{}, err
	}
	fingerprint, err := planFingerprint(&instance, memberAddresses, addressCandidates,
		addressMigration, credentialRotation)
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
	tdeKeyCreator := tdeKeyCreatorName(instance.Spec.TDE.Enabled, instance.Spec.Sites)
	siteStatuses := make([]multisitepostgresv1alpha1.SiteRevisionStatus, 0, len(instance.Spec.Sites))
	for _, site := range instance.Spec.Sites {
		registration := registrations[site.Name]
		desired := plan.SitePlan{
			SiteUID:            string(registration.UID),
			InstanceUID:        string(instance.UID),
			HubNamespace:       instance.Namespace,
			HubName:            instance.Name,
			Revision:           instance.Status.ActiveRevision,
			GeneratedAt:        now().UTC(),
			Site:               site,
			Postgres:           instance.Spec.Postgres,
			TDE:                instance.Spec.TDE,
			TDEKeyCreator:      site.Name == tdeKeyCreator,
			Backup:             instance.Spec.Backup,
			Credentials:        instance.Spec.Credentials,
			MemberAddresses:    memberAddresses,
			AddressCandidates:  addressCandidates,
			AddressMigration:   addressMigration,
			CredentialRotation: credentialRotation,
			Restore:            restorePlan,
			Upgrade:            upgradePlan,
			MajorUpgrade:       majorUpgradePlan,
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
	aggregateTopology(&instance, now())
	instance.Status.ObservedGeneration = instance.Generation
	instance.Status.Phase = "Reconciling"
	setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
		metav1.ConditionFalse, "PlansIssued", "Waiting for all sites to apply the active revision")
	if allSitesApplied(instance.Status.Sites, instance.Status.ActiveRevision) {
		setAppliedInstanceReady(&instance, restorePlan, majorUpgradePlan)
	}
	backupRequeue, err := r.reconcileBackupSchedules(ctx, &instance, now(),
		backupSchedulingReady(&instance, restorePlan, upgradePlan, majorUpgradePlan))
	if err != nil {
		setCondition(&instance.Status.Conditions, instance.Generation, "BackupReady",
			metav1.ConditionFalse, "ScheduleInvalid", err.Error())
		return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
	}
	if err := r.updateInstanceStatus(ctx, &instance); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: backupRequeue}, nil
}

func tdeKeyCreatorName(enabled bool, sites []multisitepostgresv1alpha1.PostgresSiteSpec) string {
	if !enabled {
		return ""
	}
	for _, site := range sites {
		if site.Role == multisitepostgresv1alpha1.SiteRoleData {
			return site.Name
		}
	}
	return ""
}

func backupSchedulingReady(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	restorePlan *plan.RestorePlan, upgradePlan *plan.UpgradePlan,
	majorUpgradePlan *plan.MajorUpgradePlan,
) bool {
	return conditionTrue(instance.Status.Conditions, "Ready") &&
		conditionTrue(instance.Status.Conditions, "TopologyReady") &&
		(instance.Spec.Backup == nil ||
			conditionTrue(instance.Status.Conditions, "BackupTLSReady")) &&
		restorePlan == nil && upgradePlan == nil && majorUpgradePlan == nil
}

func setAppliedInstanceReady(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	restorePlan *plan.RestorePlan, majorUpgradePlan *plan.MajorUpgradePlan,
) {
	if restorePlan != nil && restorePlan.Phase != plan.RestorePhaseVerify {
		instance.Status.Phase = "Restoring"
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "RestoreInProgress",
			"The restore target is not available until recovery and replica seeding complete")
		return
	}
	if majorUpgradePlan != nil {
		switch majorUpgradePlan.Phase {
		case plan.MajorUpgradePhaseDrain, plan.MajorUpgradePhaseStop,
			plan.MajorUpgradePhaseSnapshot, plan.MajorUpgradePhaseUpgradePrimary,
			plan.MajorUpgradePhaseStanzaUpgrade, plan.MajorUpgradePhaseStartPrimary,
			plan.MajorUpgradePhaseRollback, plan.MajorUpgradePhaseRollbackStart:
			instance.Status.Phase = "Upgrading"
			setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
				metav1.ConditionFalse, "WriteServiceUnavailable",
				"Write service is intentionally unavailable for the active major-upgrade phase")
			return
		}
	}
	if instance.Spec.Backup != nil &&
		!conditionTrue(instance.Status.Conditions, "BackupTLSReady") {
		instance.Status.Phase = "Reconciling"
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "BackupTrustPending",
			"Waiting for a common pgBackRest trust bundle across all data sites")
		return
	}
	if !conditionTrue(instance.Status.Conditions, "EtcdTLSReady") {
		instance.Status.Phase = "Reconciling"
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "EtcdTrustPending",
			"Waiting for a common etcd trust bundle across all sites")
		return
	}
	instance.Status.Phase = "Ready"
	setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
		metav1.ConditionTrue, "AllSitesReady", "All sites applied the active revision")
}

func (r *MultiSitePostgresReconciler) validateInstanceClaims(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) error {
	var instances multisitepostgresv1alpha1.MultiSitePostgresList
	if err := r.List(ctx, &instances); err != nil {
		return err
	}
	for i := range instances.Items {
		other := &instances.Items[i]
		if other.UID == instance.UID || !other.DeletionTimestamp.IsZero() {
			continue
		}
		if backupClaimsConflict(instance.Spec.Backup, other.Spec.Backup) {
			return fmt.Errorf("backup repository claim conflicts with %s/%s", other.Namespace, other.Name)
		}
		restoreSource := instance.Annotations[restoreSourceUIDAnnotation]
		if tdeClaimsConflict(instance.Spec.TDE, other.Spec.TDE) &&
			restoreSource != string(other.UID) {
			return fmt.Errorf("TDE key identity conflicts with %s/%s", other.Namespace, other.Name)
		}
	}
	return nil
}

func backupClaimsConflict(left, right *multisitepostgresv1alpha1.BackupSpec) bool {
	if left == nil || right == nil {
		return false
	}
	sameRepository := left.Repository.Type == right.Repository.Type &&
		left.Repository.Bucket == right.Repository.Bucket &&
		strings.Trim(left.Repository.Prefix, "/") == strings.Trim(right.Repository.Prefix, "/")
	sameCredentials := left.Repository.CredentialVaultRef.Mount ==
		right.Repository.CredentialVaultRef.Mount &&
		left.Repository.CredentialVaultRef.Path == right.Repository.CredentialVaultRef.Path
	return sameRepository || sameCredentials
}

func tdeClaimsConflict(left, right multisitepostgresv1alpha1.TDESpec) bool {
	if !left.Enabled || !right.Enabled || left.Vault == nil || right.Vault == nil {
		return false
	}
	return left.Vault.KVMount == right.Vault.KVMount &&
		left.Vault.KeyPath == right.Vault.KeyPath &&
		left.Vault.ProviderName == right.Vault.ProviderName &&
		left.Vault.PrincipalKeyName == right.Vault.PrincipalKeyName
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
		if instance.DeletionTimestamp.IsZero() {
			if err := controllerutil.SetControllerReference(instance, &configMap, r.Scheme); err != nil {
				return err
			}
		}
		return r.Create(ctx, &configMap)
	}
	if err != nil {
		return err
	}
	if planEnvelopeEqual(configMap.Data["envelope.json"], envelope) {
		return nil
	}
	if !instance.DeletionTimestamp.IsZero() {
		configMap.OwnerReferences = nil
	}
	configMap.Data = data
	return r.Update(ctx, &configMap)
}

func planEnvelopeEqual(currentJSON string, desired plan.Envelope) bool {
	var current plan.Envelope
	if json.Unmarshal([]byte(currentJSON), &current) != nil {
		return false
	}
	var currentPlan, desiredPlan plan.SitePlan
	if json.Unmarshal(current.Plan, &currentPlan) != nil ||
		json.Unmarshal(desired.Plan, &desiredPlan) != nil {
		return false
	}
	currentPlan.GeneratedAt = time.Time{}
	desiredPlan.GeneratedAt = time.Time{}
	currentCanonical, err := plan.Canonical(currentPlan)
	if err != nil {
		return false
	}
	desiredCanonical, err := plan.Canonical(desiredPlan)
	return err == nil && string(currentCanonical) == string(desiredCanonical)
}

func (r *MultiSitePostgresReconciler) finalize(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(instance, instanceFinalizer) {
		return ctrl.Result{}, nil
	}
	if instance.Annotations["multisite-postgres.dev/force-orphan"] == "true" {
		if err := r.deletePlanConfigMaps(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(instance, instanceFinalizer)
		return ctrl.Result{}, r.Update(ctx, instance)
	}
	if instance.Status.PlanFingerprint != "deleting" {
		if instance.Spec.DeletionPolicy == multisitepostgresv1alpha1.DeletionPolicyDelete &&
			!conditionTrue(instance.Status.Conditions, "BackupReady") {
			instance.Status.Phase = "Deleting"
			setCondition(&instance.Status.Conditions, instance.Generation, "DeletionBlocked",
				metav1.ConditionTrue, "BackupNotVerified",
				"Delete policy requires a verified backup before remote cleanup begins")
			return ctrl.Result{}, r.updateInstanceStatus(ctx, instance)
		}
		if err := r.issueDeletionPlans(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
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
	if err := r.deletePlanConfigMaps(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(instance, instanceFinalizer)
	return ctrl.Result{}, r.Update(ctx, instance)
}

func (r *MultiSitePostgresReconciler) issueDeletionPlans(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) error {
	systemNamespace := r.SystemNamespace
	if systemNamespace == "" {
		systemNamespace = "mspsql-system"
	}
	privateKey, err := ensureSigningKey(ctx, r.Client, systemNamespace)
	if err != nil {
		return err
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	deletionPolicy := instance.Spec.DeletionPolicy
	if deletionPolicy == "" {
		deletionPolicy = multisitepostgresv1alpha1.DeletionPolicyRetain
	}
	instance.Status.ActiveRevision++
	if instance.Status.ActiveRevision == 0 {
		instance.Status.ActiveRevision = 1
	}
	memberAddresses := make(map[string]string)
	for _, siteStatus := range instance.Status.Sites {
		maps.Copy(memberAddresses, siteStatus.Addresses)
	}
	statuses := make([]multisitepostgresv1alpha1.SiteRevisionStatus, 0, len(instance.Spec.Sites))
	for _, site := range instance.Spec.Sites {
		var registration multisitepostgresv1alpha1.SiteRegistration
		if err := r.Get(ctx, client.ObjectKey{Name: site.SiteRegistrationRef}, &registration); err != nil {
			return err
		}
		envelope, err := plan.Sign(privateKey, plan.SitePlan{
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
			Credentials:     instance.Spec.Credentials,
			MemberAddresses: memberAddresses,
			Deletion:        &plan.DeletionPlan{Policy: deletionPolicy},
		})
		if err != nil {
			return err
		}
		if err := r.reconcilePlan(ctx, instance, site.Name, string(registration.UID), envelope); err != nil {
			return err
		}
		status := previousSiteStatus(instance.Status.Sites, site.Name)
		status.Name = site.Name
		status.SiteRegistrationRef = site.SiteRegistrationRef
		status.DesiredRevision = instance.Status.ActiveRevision
		status.Phase = "DeletionPlanIssued"
		statuses = append(statuses, status)
	}
	instance.Status.Sites = statuses
	instance.Status.PlanFingerprint = "deleting"
	instance.Status.Phase = "Deleting"
	setCondition(&instance.Status.Conditions, instance.Generation, "DeletionBlocked",
		metav1.ConditionTrue, "AwaitingSites", "Waiting for all sites to acknowledge remote cleanup")
	return r.updateInstanceStatus(ctx, instance)
}

func (r *MultiSitePostgresReconciler) deletePlanConfigMaps(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) error {
	var plans corev1.ConfigMapList
	if err := r.List(ctx, &plans, client.InNamespace(instance.Namespace), client.MatchingLabels{
		"multisite-postgres.dev/instance-uid": string(instance.UID),
	}); err != nil {
		return err
	}
	for i := range plans.Items {
		if err := r.Delete(ctx, &plans.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *MultiSitePostgresReconciler) updateInstanceStatus(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) error {
	desired := instance.Status.DeepCopy()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current multisitepostgresv1alpha1.MultiSitePostgres
		if err := r.Get(ctx, client.ObjectKeyFromObject(instance), &current); err != nil {
			return err
		}
		mergeReconciledStatus(&current.Status, desired)
		return r.Status().Update(ctx, &current)
	})
}

func mergeReconciledStatus(current, desired *multisitepostgresv1alpha1.MultiSitePostgresStatus) {
	current.ObservedGeneration = desired.ObservedGeneration
	if desired.ActiveRevision > current.ActiveRevision {
		current.ActiveRevision = desired.ActiveRevision
	}
	current.PlanFingerprint = desired.PlanFingerprint
	current.Phase = desired.Phase
	current.Primary = desired.Primary
	current.SynchronousStandbys = append([]string(nil), desired.SynchronousStandbys...)
	current.BackupSchedules = append([]multisitepostgresv1alpha1.BackupScheduleStatus(nil),
		desired.BackupSchedules...)
	for _, condition := range desired.Conditions {
		meta.SetStatusCondition(&current.Conditions, condition)
	}
	current.Sites = mergeReconciledSites(current.Sites, desired.Sites)
}

func mergeReconciledSites(current, desired []multisitepostgresv1alpha1.SiteRevisionStatus,
) []multisitepostgresv1alpha1.SiteRevisionStatus {
	merged := make([]multisitepostgresv1alpha1.SiteRevisionStatus, 0, len(desired))
	for _, wanted := range desired {
		observed := previousSiteStatus(current, wanted.Name)
		if observed.Name == "" {
			observed = wanted
		}
		observed.Name = wanted.Name
		observed.SiteRegistrationRef = wanted.SiteRegistrationRef
		observed.DesiredRevision = wanted.DesiredRevision
		if wanted.Phase == "DeletionPlanIssued" && observed.Phase != "Deleted" {
			observed.Phase = wanted.Phase
		}
		merged = append(merged, observed)
	}
	return merged
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

func aggregateTopology(instance *multisitepostgresv1alpha1.MultiSitePostgres, now time.Time) {
	dataSites := 0
	seedRestore := instance.Annotations[restorePhaseAnnotation] == string(plan.RestorePhaseSeed)
	majorPhase := plan.MajorUpgradePhase(instance.Annotations[upgradePhaseAnnotation])
	if majorPhase == plan.MajorUpgradePhaseStop ||
		majorPhase == plan.MajorUpgradePhaseSnapshot ||
		majorPhase == plan.MajorUpgradePhaseUpgradePrimary ||
		majorPhase == plan.MajorUpgradePhaseStanzaUpgrade ||
		majorPhase == plan.MajorUpgradePhaseRollback {
		instance.Status.Primary = ""
		instance.Status.SynchronousStandbys = nil
		setCondition(&instance.Status.Conditions, instance.Generation, "TopologyReady",
			metav1.ConditionFalse, "PostgreSQLStopped",
			"Topology observation is suspended while PostgreSQL is intentionally stopped")
		return
	}
	majorPrimaryOnly := majorPhase == plan.MajorUpgradePhaseStartPrimary ||
		majorPhase == plan.MajorUpgradePhaseRestoreWrites
	if seedRestore {
		dataSites = 1
	}
	if majorPrimaryOnly {
		dataSites = 1
	}
	for _, site := range instance.Spec.Sites {
		if !seedRestore && !majorPrimaryOnly && site.Role == multisitepostgresv1alpha1.SiteRoleData {
			dataSites++
		}
	}
	primaryCounts := map[string]int{}
	var observed []multisitepostgresv1alpha1.SiteRevisionStatus
	for _, site := range instance.Status.Sites {
		if majorPrimaryOnly &&
			!strings.HasPrefix(instance.Annotations[upgradeFromAnnotation], "postgres-"+site.Name+"-") {
			continue
		}
		if site.Primary == "" || site.TopologyObservedAt == nil ||
			now.Sub(site.TopologyObservedAt.Time) > 2*time.Minute {
			continue
		}
		primaryCounts[site.Primary]++
		observed = append(observed, site)
	}
	if len(observed) != dataSites {
		instance.Status.Primary = ""
		instance.Status.SynchronousStandbys = nil
		setCondition(&instance.Status.Conditions, instance.Generation, "TopologyReady",
			metav1.ConditionFalse, "InsufficientObservations",
			fmt.Sprintf("Received %d of %d current data-site topology observations", len(observed), dataSites))
		return
	}
	if len(primaryCounts) != 1 {
		instance.Status.Primary = ""
		instance.Status.SynchronousStandbys = nil
		setCondition(&instance.Status.Conditions, instance.Generation, "TopologyReady",
			metav1.ConditionFalse, "ConflictingObservations",
			"Data sites disagree about the current Patroni leader")
		return
	}
	for primary := range primaryCounts {
		instance.Status.Primary = primary
	}
	standbyCounts := map[string]int{}
	for _, site := range observed {
		for _, standby := range site.SynchronousStandbys {
			standbyCounts[standby]++
		}
	}
	instance.Status.SynchronousStandbys = nil
	for standby, count := range standbyCounts {
		if count == dataSites {
			instance.Status.SynchronousStandbys = append(instance.Status.SynchronousStandbys, standby)
		}
	}
	slices.Sort(instance.Status.SynchronousStandbys)
	setCondition(&instance.Status.Conditions, instance.Generation, "TopologyReady",
		metav1.ConditionTrue, "ObserverConsensus", "All data sites report the same Patroni leader")
}

// SetupWithManager sets up the controller with the Manager.
func (r *MultiSitePostgresReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&multisitepostgresv1alpha1.MultiSitePostgres{}, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
			predicate.NewPredicateFuncs(func(object client.Object) bool {
				return !object.GetDeletionTimestamp().IsZero()
			}),
		))).
		Owns(&corev1.ConfigMap{}).
		Watches(&multisitepostgresv1alpha1.SiteRegistration{},
			handler.EnqueueRequestsFromMapFunc(r.instancesForRegistration)).
		Named("multisitepostgres").
		Complete(r)
}

func (r *MultiSitePostgresReconciler) instancesForRegistration(ctx context.Context,
	object client.Object,
) []reconcile.Request {
	var instances multisitepostgresv1alpha1.MultiSitePostgresList
	if err := r.List(ctx, &instances); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range instances.Items {
		instance := &instances.Items[i]
		for _, site := range instance.Spec.Sites {
			if site.SiteRegistrationRef == object.GetName() {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(instance)})
				break
			}
		}
	}
	return requests
}

func planFingerprint(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	addresses, candidates map[string]string, migration *plan.AddressMigrationPlan,
	credentialRotation *plan.CredentialRotationPlan,
) (string, error) {
	payload, err := json.Marshal(struct {
		Generation         int64
		Addresses          map[string]string
		AddressCandidates  map[string]string
		AddressMigration   *plan.AddressMigrationPlan
		CredentialRotation *plan.CredentialRotationPlan
		RestoreUID         string
		RestorePhase       string
		UpgradeUID         string
		UpgradePhase       string
		UpgradeMember      string
		UpgradedMembers    string
	}{
		Generation:         instance.Generation,
		Addresses:          addresses,
		AddressCandidates:  candidates,
		AddressMigration:   migration,
		CredentialRotation: credentialRotation,
		RestoreUID:         instance.Annotations[restoreUIDAnnotation],
		RestorePhase:       instance.Annotations[restorePhaseAnnotation],
		UpgradeUID:         instance.Annotations[upgradeUIDAnnotation],
		UpgradePhase:       instance.Annotations[upgradePhaseAnnotation],
		UpgradeMember:      instance.Annotations[upgradeMemberAnnotation],
		UpgradedMembers:    instance.Annotations[upgradeMembersAnnotation],
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (r *MultiSitePostgresReconciler) credentialRotationPlan(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
	addressMigration *plan.AddressMigrationPlan, restorePlan *plan.RestorePlan,
	upgradePlan *plan.UpgradePlan, majorUpgradePlan *plan.MajorUpgradePlan,
) (*plan.CredentialRotationPlan, error) {
	active, err := r.activeSitePlan(ctx, instance)
	if err != nil {
		return nil, err
	}
	if active.CredentialRotation != nil &&
		!allSitesApplied(instance.Status.Sites, instance.Status.ActiveRevision) {
		return active.CredentialRotation, nil
	}
	if addressMigration != nil || restorePlan != nil || upgradePlan != nil || majorUpgradePlan != nil {
		return nil, nil
	}
	version, staged := commonStagedCredentialVersion(instance)
	if !staged {
		return nil, nil
	}
	previousVersion := activeCredentialVersion(instance)
	if active.CredentialRotation != nil {
		previousVersion = active.CredentialRotation.PreviousVersion
	}
	if previousVersion < 1 || previousVersion == version {
		return nil, nil
	}
	if !credentialCatalogUpdated(instance, version) {
		return &plan.CredentialRotationPlan{
			Version: version, PreviousVersion: previousVersion,
			Phase: plan.CredentialRotationPhaseCatalog,
		}, nil
	}
	updated := credentialUpdatedMembers(instance, version)
	for _, member := range credentialMemberOrder(instance) {
		if !slices.Contains(updated, member) {
			return &plan.CredentialRotationPlan{
				Version: version, PreviousVersion: previousVersion,
				Phase:        plan.CredentialRotationPhaseMember,
				TargetMember: member, UpdatedMembers: updated,
			}, nil
		}
	}
	if !previousCredentialsRevoked(instance, version) {
		return &plan.CredentialRotationPlan{
			Version: version, PreviousVersion: previousVersion,
			Phase: plan.CredentialRotationPhaseRevoke, UpdatedMembers: updated,
		}, nil
	}
	return &plan.CredentialRotationPlan{
		Version: version, PreviousVersion: previousVersion,
		Phase:          plan.CredentialRotationPhaseFinalize,
		UpdatedMembers: updated,
	}, nil
}

func previousCredentialsRevoked(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	version int64,
) bool {
	for _, site := range instance.Status.Sites {
		if !strings.HasPrefix(instance.Status.Primary, "postgres-"+site.Name+"-") {
			continue
		}
		condition := statusCondition(site.Conditions, "PreviousCredentialsRevoked")
		return condition != nil && condition.Status == metav1.ConditionTrue &&
			condition.Message == strconv.FormatInt(version, 10)
	}
	return false
}

func activeCredentialVersion(instance *multisitepostgresv1alpha1.MultiSitePostgres) int64 {
	var version int64
	for _, site := range instance.Status.Sites {
		if siteRoleForName(instance, site.Name) == multisitepostgresv1alpha1.SiteRoleWitness {
			continue
		}
		condition := statusCondition(site.Conditions, "PostgresCredentialsActive")
		if condition == nil || condition.Status != metav1.ConditionTrue {
			return 0
		}
		siteVersion, err := strconv.ParseInt(condition.Message, 10, 64)
		if err != nil || siteVersion < 1 || version != 0 && siteVersion != version {
			return 0
		}
		version = siteVersion
	}
	return version
}

func commonStagedCredentialVersion(instance *multisitepostgresv1alpha1.MultiSitePostgres) (int64, bool) {
	var version int64
	applicable := 0
	for _, site := range instance.Status.Sites {
		if siteRoleForName(instance, site.Name) == multisitepostgresv1alpha1.SiteRoleWitness {
			continue
		}
		applicable++
		condition := statusCondition(site.Conditions, "CredentialRotationPending")
		if condition == nil || condition.Status != metav1.ConditionTrue {
			return 0, false
		}
		siteVersion, err := strconv.ParseInt(condition.Message, 10, 64)
		if err != nil || siteVersion < 1 || version != 0 && siteVersion != version {
			return 0, false
		}
		version = siteVersion
	}
	return version, applicable > 0
}

func credentialCatalogUpdated(instance *multisitepostgresv1alpha1.MultiSitePostgres, version int64) bool {
	for _, site := range instance.Status.Sites {
		if !strings.HasPrefix(instance.Status.Primary, "postgres-"+site.Name+"-") {
			continue
		}
		condition := statusCondition(site.Conditions, "CredentialCatalogUpdated")
		return condition != nil && condition.Status == metav1.ConditionTrue &&
			condition.Message == strconv.FormatInt(version, 10)
	}
	return false
}

func credentialUpdatedMembers(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	version int64,
) []string {
	set := map[string]struct{}{}
	prefix := strconv.FormatInt(version, 10) + ":"
	for _, site := range instance.Status.Sites {
		condition := statusCondition(site.Conditions, "CredentialMembersUpdated")
		if condition == nil || condition.Status != metav1.ConditionTrue ||
			!strings.HasPrefix(condition.Message, prefix) {
			continue
		}
		for _, member := range splitMembers(strings.TrimPrefix(condition.Message, prefix)) {
			set[member] = struct{}{}
		}
	}
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	slices.Sort(members)
	return members
}

func credentialMemberOrder(instance *multisitepostgresv1alpha1.MultiSitePostgres) []string {
	order := slices.Clone(instance.Status.SynchronousStandbys)
	slices.Sort(order)
	for _, member := range expectedAddressMembers(instance.Spec.Sites) {
		if strings.HasPrefix(member, "postgres-") && member != instance.Status.Primary &&
			!slices.Contains(order, member) {
			order = append(order, member)
		}
	}
	if instance.Status.Primary != "" {
		order = append(order, instance.Status.Primary)
	}
	return order
}

func statusCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func siteRoleForName(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	name string,
) multisitepostgresv1alpha1.SiteRole {
	for _, site := range instance.Spec.Sites {
		if site.Name == name {
			return site.Role
		}
	}
	return ""
}

func (r *MultiSitePostgresReconciler) addressPlan(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (map[string]string, map[string]string, *plan.AddressMigrationPlan, error) {
	observed := make(map[string]string)
	for _, site := range instance.Status.Sites {
		maps.Copy(observed, site.Addresses)
	}
	previous, err := r.activeSitePlan(ctx, instance)
	if err != nil {
		return nil, nil, nil, err
	}
	if previous.AddressMigration != nil &&
		!allSitesApplied(instance.Status.Sites, instance.Status.ActiveRevision) {
		return previous.MemberAddresses, observed, previous.AddressMigration, nil
	}
	expected := expectedAddressMembers(instance.Spec.Sites)
	if !addressesComplete(previous.MemberAddresses, expected) {
		return observed, observed, nil, nil
	}
	addresses := make(map[string]string, len(previous.MemberAddresses))
	maps.Copy(addresses, previous.MemberAddresses)
	var changed []string
	for _, member := range expected {
		if current := observed[member]; current != "" && current != previous.MemberAddresses[member] {
			changed = append(changed, member)
		}
	}
	if len(changed) == 0 {
		return addresses, observed, nil, nil
	}
	slices.Sort(changed)
	member := changed[0]
	oldAddress := previous.MemberAddresses[member]
	newAddress := observed[member]
	addresses[member] = newAddress
	sum := sha256.Sum256([]byte(string(instance.UID) + "\x00" + member + "\x00" +
		oldAddress + "\x00" + newAddress))
	return addresses, observed, &plan.AddressMigrationPlan{
		OperationUID: hex.EncodeToString(sum[:16]),
		Member:       member,
		OldAddress:   oldAddress,
		NewAddress:   newAddress,
	}, nil
}

func (r *MultiSitePostgresReconciler) activeSitePlan(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (plan.SitePlan, error) {
	if len(instance.Spec.Sites) == 0 {
		return plan.SitePlan{}, nil
	}
	var configMap corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{
		Namespace: instance.Namespace,
		Name:      fmt.Sprintf("mspsql-plan-%s-%s", instance.Name, instance.Spec.Sites[0].Name),
	}, &configMap)
	if apierrors.IsNotFound(err) {
		return plan.SitePlan{}, nil
	}
	if err != nil {
		return plan.SitePlan{}, err
	}
	var envelope plan.Envelope
	if err := json.Unmarshal([]byte(configMap.Data["envelope.json"]), &envelope); err != nil {
		return plan.SitePlan{}, fmt.Errorf("decode active site plan envelope: %w", err)
	}
	var desired plan.SitePlan
	if err := json.Unmarshal(envelope.Plan, &desired); err != nil {
		return plan.SitePlan{}, fmt.Errorf("decode active site plan: %w", err)
	}
	return desired, nil
}

func expectedAddressMembers(sites []multisitepostgresv1alpha1.PostgresSiteSpec) []string {
	var members []string
	for _, site := range sites {
		for ordinal := int32(0); ordinal < site.Components.EtcdReplicas; ordinal++ {
			members = append(members, fmt.Sprintf("etcd-%s-%d", site.Name, ordinal))
		}
		for ordinal := int32(0); ordinal < site.Components.PostgresReplicas; ordinal++ {
			members = append(members, fmt.Sprintf("postgres-%s-%d", site.Name, ordinal))
		}
		if site.Components.PgpoolReplicas > 0 {
			members = append(members, "pgpool-"+site.Name)
		}
	}
	slices.Sort(members)
	return members
}

func addressesComplete(addresses map[string]string, expected []string) bool {
	if len(expected) == 0 {
		return false
	}
	for _, member := range expected {
		if addresses[member] == "" {
			return false
		}
	}
	return true
}

func (r *MultiSitePostgresReconciler) upgradePlans(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (*plan.UpgradePlan, *plan.MajorUpgradePlan, error) {
	upgradeName := instance.Annotations[upgradeNameAnnotation]
	if upgradeName == "" {
		return nil, nil, nil
	}
	var upgrade multisitepostgresv1alpha1.PostgresUpgrade
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: instance.Namespace, Name: upgradeName,
	}, &upgrade); err != nil {
		return nil, nil, fmt.Errorf("read owning upgrade: %w", err)
	}
	phaseValue := instance.Annotations[upgradePhaseAnnotation]
	if strings.HasPrefix(phaseValue, "Major") {
		phase := plan.MajorUpgradePhase(phaseValue)
		switch phase {
		case plan.MajorUpgradePhasePreflight, plan.MajorUpgradePhaseDrain,
			plan.MajorUpgradePhaseStop,
			plan.MajorUpgradePhaseSnapshot, plan.MajorUpgradePhaseUpgradePrimary,
			plan.MajorUpgradePhaseStanzaUpgrade,
			plan.MajorUpgradePhaseStartPrimary, plan.MajorUpgradePhaseRestoreWrites,
			plan.MajorUpgradePhaseReplicas,
			plan.MajorUpgradePhaseFinalize, plan.MajorUpgradePhaseRollback,
			plan.MajorUpgradePhaseRollbackStart,
			plan.MajorUpgradePhaseRollbackRestoreWrites:
		default:
			return nil, nil, fmt.Errorf("unsupported major-upgrade phase %q", phase)
		}
		policies := make(map[string]multisitepostgresv1alpha1.StorageRollbackPolicy)
		for _, site := range instance.Spec.Sites {
			if site.Role != multisitepostgresv1alpha1.SiteRoleData || site.Storage.Postgres == nil {
				continue
			}
			var registration multisitepostgresv1alpha1.SiteRegistration
			if err := r.Get(ctx, client.ObjectKey{Name: site.SiteRegistrationRef}, &registration); err != nil {
				return nil, nil, fmt.Errorf("read site %s registration: %w", site.Name, err)
			}
			policy, found := rollbackPolicy(registration.Spec.StorageRollbackPolicies,
				site.Storage.Postgres.StorageClassName)
			if !found {
				return nil, nil, fmt.Errorf("site %s rollback policy is unavailable", site.Name)
			}
			policies[site.Name] = policy
		}
		sourceMajor, err := strconv.ParseInt(instance.Annotations[upgradeSourceMajorAnnotation], 10, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("major upgrade source version is invalid")
		}
		return nil, &plan.MajorUpgradePlan{
			OperationUID:      string(upgrade.UID),
			Phase:             phase,
			Primary:           instance.Annotations[upgradeFromAnnotation],
			SourceMajor:       int32(sourceMajor),
			TargetMajor:       upgrade.Spec.TargetMajorVersion,
			TargetImage:       upgrade.Spec.TargetImage,
			UpgradeImage:      upgrade.Spec.UpgradeImage,
			RollbackRetention: upgrade.Spec.RollbackRetention.Duration,
			RollbackPolicies:  policies,
		}, nil
	}
	phase := plan.UpgradePhase(instance.Annotations[upgradePhaseAnnotation])
	switch phase {
	case plan.UpgradePhaseMember, plan.UpgradePhaseSwitchover, plan.UpgradePhaseFinalize:
	default:
		return nil, nil, fmt.Errorf("unsupported minor-upgrade phase %q", phase)
	}
	return &plan.UpgradePlan{
		OperationUID:    string(upgrade.UID),
		TargetImage:     upgrade.Spec.TargetImage,
		TargetMember:    instance.Annotations[upgradeMemberAnnotation],
		UpgradedMembers: splitMembers(instance.Annotations[upgradeMembersAnnotation]),
		FromPrimary:     instance.Annotations[upgradeFromAnnotation],
		Candidate:       instance.Annotations[upgradeCandidateAnnotation],
		Phase:           phase,
	}, nil, nil
}

func (r *MultiSitePostgresReconciler) restorePlan(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (*plan.RestorePlan, error) {
	restoreName := instance.Annotations[restoreNameAnnotation]
	if restoreName == "" || instance.Annotations[restorePhaseAnnotation] == "Completed" {
		return nil, nil
	}
	var restore multisitepostgresv1alpha1.PostgresRestore
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: instance.Namespace, Name: restoreName,
	}, &restore); err != nil {
		return nil, fmt.Errorf("read owning restore: %w", err)
	}
	var source multisitepostgresv1alpha1.MultiSitePostgres
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: instance.Namespace, Name: restore.Spec.SourceInstanceRef,
	}, &source); err != nil {
		return nil, fmt.Errorf("read restore source: %w", err)
	}
	if source.Spec.Backup == nil {
		return nil, fmt.Errorf("restore source has no backup repository")
	}
	seedSite, seedMember, err := selectRestoreSeed(instance.Spec.Sites)
	if err != nil {
		return nil, err
	}
	phase := plan.RestorePhase(instance.Annotations[restorePhaseAnnotation])
	switch phase {
	case plan.RestorePhaseSeed, plan.RestorePhaseReplicas, plan.RestorePhaseVerify:
	default:
		return nil, fmt.Errorf("unsupported restore phase %q", phase)
	}
	return &plan.RestorePlan{
		OperationUID:      string(restore.UID),
		SourceInstanceUID: string(source.UID),
		SourceBackup:      *source.Spec.Backup.DeepCopy(),
		TargetTime:        restore.Spec.TargetTime.Time,
		BackupSet:         restore.Spec.BackupSet,
		SeedSite:          seedSite,
		SeedMember:        seedMember,
		Phase:             phase,
	}, nil
}
