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
	upgradePlan, err := r.upgradePlan(ctx, &instance)
	if err != nil {
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "UpgradeContractInvalid", err.Error())
		instance.Status.Phase = "Upgrading"
		return ctrl.Result{}, r.updateInstanceStatus(ctx, &instance)
	}
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
		maps.Copy(memberAddresses, siteStatus.Addresses)
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
			Credentials:     instance.Spec.Credentials,
			MemberAddresses: memberAddresses,
			Restore:         restorePlan,
			Upgrade:         upgradePlan,
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
		setAppliedInstanceReady(&instance, restorePlan)
	}
	backupRequeue, err := r.reconcileBackupSchedules(ctx, &instance, now(),
		backupSchedulingReady(&instance, restorePlan, upgradePlan))
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

func backupSchedulingReady(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	restorePlan *plan.RestorePlan, upgradePlan *plan.UpgradePlan,
) bool {
	return conditionTrue(instance.Status.Conditions, "Ready") &&
		conditionTrue(instance.Status.Conditions, "TopologyReady") &&
		(instance.Spec.Backup == nil ||
			conditionTrue(instance.Status.Conditions, "BackupTLSReady")) &&
		restorePlan == nil && upgradePlan == nil
}

func setAppliedInstanceReady(instance *multisitepostgresv1alpha1.MultiSitePostgres,
	restorePlan *plan.RestorePlan,
) {
	if restorePlan != nil && restorePlan.Phase != plan.RestorePhaseVerify {
		instance.Status.Phase = "Restoring"
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "RestoreInProgress",
			"The restore target is not available until recovery and replica seeding complete")
		return
	}
	if instance.Spec.Backup != nil &&
		!conditionTrue(instance.Status.Conditions, "BackupTLSReady") {
		instance.Status.Phase = "Reconciling"
		setCondition(&instance.Status.Conditions, instance.Generation, "Ready",
			metav1.ConditionFalse, "BackupTrustPending",
			"Waiting for a common pgBackRest trust bundle across all data sites")
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
	if configMap.Data["envelope.json"] == data["envelope.json"] {
		return nil
	}
	if !instance.DeletionTimestamp.IsZero() {
		configMap.OwnerReferences = nil
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

func aggregateTopology(instance *multisitepostgresv1alpha1.MultiSitePostgres, now time.Time) {
	dataSites := 0
	seedRestore := instance.Annotations[restorePhaseAnnotation] == string(plan.RestorePhaseSeed)
	if seedRestore {
		dataSites = 1
	}
	for _, site := range instance.Spec.Sites {
		if !seedRestore && site.Role == multisitepostgresv1alpha1.SiteRoleData {
			dataSites++
		}
	}
	primaryCounts := map[string]int{}
	var observed []multisitepostgresv1alpha1.SiteRevisionStatus
	for _, site := range instance.Status.Sites {
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
		Generation      int64
		Addresses       map[string]map[string]string
		RestoreUID      string
		RestorePhase    string
		UpgradeUID      string
		UpgradePhase    string
		UpgradeMember   string
		UpgradedMembers string
	}{
		Generation:      instance.Generation,
		RestoreUID:      instance.Annotations[restoreUIDAnnotation],
		RestorePhase:    instance.Annotations[restorePhaseAnnotation],
		UpgradeUID:      instance.Annotations[upgradeUIDAnnotation],
		UpgradePhase:    instance.Annotations[upgradePhaseAnnotation],
		UpgradeMember:   instance.Annotations[upgradeMemberAnnotation],
		UpgradedMembers: instance.Annotations[upgradeMembersAnnotation],
		Addresses: func() map[string]map[string]string {
			addresses := make(map[string]map[string]string, len(instance.Spec.Sites))
			for _, desiredSite := range instance.Spec.Sites {
				addresses[desiredSite.Name] = map[string]string{}
			}
			for _, observedSite := range instance.Status.Sites {
				if observedSite.Addresses != nil {
					addresses[observedSite.Name] = observedSite.Addresses
				}
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

func (r *MultiSitePostgresReconciler) upgradePlan(ctx context.Context,
	instance *multisitepostgresv1alpha1.MultiSitePostgres,
) (*plan.UpgradePlan, error) {
	upgradeName := instance.Annotations[upgradeNameAnnotation]
	if upgradeName == "" {
		return nil, nil
	}
	var upgrade multisitepostgresv1alpha1.PostgresUpgrade
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: instance.Namespace, Name: upgradeName,
	}, &upgrade); err != nil {
		return nil, fmt.Errorf("read owning upgrade: %w", err)
	}
	if upgrade.Spec.TargetMajorVersion != instance.Spec.Postgres.MajorVersion {
		return nil, fmt.Errorf("major upgrades use the disruptive upgrade state machine")
	}
	phase := plan.UpgradePhase(instance.Annotations[upgradePhaseAnnotation])
	switch phase {
	case plan.UpgradePhaseMember, plan.UpgradePhaseSwitchover, plan.UpgradePhaseFinalize:
	default:
		return nil, fmt.Errorf("unsupported minor-upgrade phase %q", phase)
	}
	return &plan.UpgradePlan{
		OperationUID:    string(upgrade.UID),
		TargetImage:     upgrade.Spec.TargetImage,
		TargetMember:    instance.Annotations[upgradeMemberAnnotation],
		UpgradedMembers: splitMembers(instance.Annotations[upgradeMembersAnnotation]),
		FromPrimary:     instance.Annotations[upgradeFromAnnotation],
		Candidate:       instance.Annotations[upgradeCandidateAnnotation],
		Phase:           phase,
	}, nil
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
