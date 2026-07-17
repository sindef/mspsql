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
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
)

type ApplyResult struct {
	Phase               string
	Addresses           map[string]string
	Primary             string
	SynchronousStandbys []string
	Conditions          []metav1.Condition
}

type Reconciler struct {
	Client    client.Client
	Renderer  Renderer
	Secrets   *SecretMaterializer
	Topology  *PatroniObserver
	HubDomain string
	SiteUID   string
	applyLock sync.Mutex
}

func (r *Reconciler) Apply(ctx context.Context, desired, previous plan.SitePlan,
	connected bool,
) (ApplyResult, error) {
	r.applyLock.Lock()
	defer r.applyLock.Unlock()
	result := ApplyResult{Phase: "CreatingNamespaces", Addresses: map[string]string{}}
	if desired.Deletion != nil {
		if !connected {
			result.Phase = "WaitingForHub"
			setLocalCondition(&result.Conditions, "Deleted", metav1.ConditionFalse,
				"HubRequired", "Deletion is blocked while disconnected from the hub")
			return result, nil
		}
		return r.deleteInstance(ctx, desired)
	}
	if err := EnsureNamespace(ctx, r.Client, desired.Site.Namespace, r.HubDomain, r.SiteUID,
		desired.InstanceUID, connected); err != nil {
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse,
			"NamespaceOwnershipConflict", err.Error())
		return result, err
	}
	if err := r.apply(ctx, r.Renderer.ServiceAccount(desired)); err != nil {
		return result, err
	}
	if r.Secrets != nil {
		result.Phase = "ResolvingSecrets"
		if err := r.reconcileSecrets(ctx, &desired, &result); err != nil {
			return result, err
		}
	}

	result.Phase = "AllocatingLoadBalancers"
	ready, err := r.reconcileLoadBalancers(ctx, desired, connected, &result)
	if err != nil || !ready {
		return result, err
	}

	if !connected && plan.Classify(previous, desired) == plan.MutationCoordinated {
		result.Phase = "WaitingForHub"
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse,
			"HubRequired", "The desired revision contains globally coordinated changes")
		return result, nil
	}
	if connected {
		desired.MemberAddresses = fillMissingAddresses(desired.MemberAddresses, result.Addresses)
		desired.AddressCandidates = mergeAddresses(desired.AddressCandidates, result.Addresses)
		result.Phase = "IssuingCertificates"
		ready, err := r.reconcileCertificates(ctx, desired, &result)
		if err != nil || !ready {
			return result, err
		}
	}

	if ready, err := r.prepareOperations(ctx, &desired, &result); err != nil || !ready {
		return result, err
	}

	result.Phase = "ReconcilingWorkloads"
	desired.MemberAddresses = fillMissingAddresses(desired.MemberAddresses, result.Addresses)
	objects, err := r.Renderer.Workloads(desired)
	if err != nil {
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse, "GlobalAddressesPending", err.Error())
		return result, nil
	}
	if ready, err := r.reconcileWorkloads(ctx, desired, objects, &result); err != nil || !ready {
		return result, err
	}
	setLocalCondition(&result.Conditions, "EtcdQuorate", metav1.ConditionTrue,
		"AllMembersHealthy", "All etcd member readiness checks are passing")
	if desired.AddressMigration != nil {
		setLocalCondition(&result.Conditions, "AddressMigrated", metav1.ConditionTrue,
			"ConsumersUpdated", fmt.Sprintf("%s migrated from %s to %s",
				desired.AddressMigration.Member, desired.AddressMigration.OldAddress,
				desired.AddressMigration.NewAddress))
	}
	r.setCredentialMemberCondition(desired, &result)
	if err := r.setDataPlaneConditions(ctx, desired, &result); err != nil {
		return result, err
	}
	if result.Phase == "RotatingCredentials" {
		return result, nil
	}
	if err := r.pruneStaleObjects(ctx, desired); err != nil {
		return result, err
	}
	if err := r.cleanupExpiredRollback(ctx, desired, time.Now()); err != nil {
		return result, err
	}
	result.Phase = "Ready"
	setLocalCondition(&result.Conditions, "Ready", metav1.ConditionTrue,
		"DesiredStateApplied", "All locally managed resources match the signed plan")
	return result, nil
}

func (r *Reconciler) prepareOperations(ctx context.Context, desired *plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	if desired.AddressMigration != nil {
		result.Phase = "MigratingAddress"
		if ready, err := r.reconcileAddressMigration(ctx, *desired, result); err != nil || !ready {
			return ready, err
		}
	}
	if desired.CredentialRotation != nil {
		if ready, err := r.prepareCredentialRotation(ctx, desired, result); err != nil || !ready {
			return ready, err
		}
	}
	if desired.MajorUpgrade != nil {
		if ready, err := r.prepareMajorUpgrade(ctx, *desired, result); err != nil || !ready {
			return ready, err
		}
	}
	return true, nil
}

func (r *Reconciler) reconcileWorkloads(ctx context.Context, desired plan.SitePlan,
	objects []client.Object, result *ApplyResult,
) (bool, error) {
	for _, object := range objects {
		if err := r.apply(ctx, object); err != nil {
			return false, err
		}
	}
	ready, message, err := r.workloadsReady(ctx, objects)
	if err != nil {
		return false, err
	}
	if !ready {
		setLocalCondition(&result.Conditions, "Ready", metav1.ConditionFalse,
			"WorkloadsProgressing", message)
		setMajorUpgradeProgressCondition(desired, result, message)
		return false, nil
	}
	if desired.MajorUpgrade != nil {
		setLocalCondition(&result.Conditions, "MajorUpgradeBlocked", metav1.ConditionFalse,
			"PhaseAccepted", "The local major-upgrade phase completed")
	}
	return true, nil
}

func setMajorUpgradeProgressCondition(desired plan.SitePlan, result *ApplyResult, message string) {
	if desired.MajorUpgrade == nil {
		return
	}
	reason := "WorkloadsPending"
	if strings.Contains(message, " failed:") {
		switch desired.MajorUpgrade.Phase {
		case plan.MajorUpgradePhasePreflight:
			reason = "PreflightRejected"
		case plan.MajorUpgradePhaseStartPrimary:
			reason = "PrimaryAcceptanceFailed"
		case plan.MajorUpgradePhaseRollbackStart:
			reason = "RollbackAcceptanceFailed"
		default:
			reason = "WorkloadsFailed"
		}
	}
	setLocalCondition(&result.Conditions, "MajorUpgradeBlocked", metav1.ConditionTrue, reason, message)
}

func (r *Reconciler) reconcileLoadBalancers(ctx context.Context, desired plan.SitePlan,
	connected bool, result *ApplyResult,
) (bool, error) {
	for _, object := range r.Renderer.LoadBalancers(desired) {
		if !connected {
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
				if apierrors.IsNotFound(err) {
					setLocalCondition(&result.Conditions, "AddressChangeBlocked", metav1.ConditionTrue,
						"HubDisconnected", "LoadBalancer Services are not recreated while disconnected")
				}
				return false, err
			}
		} else if err := r.apply(ctx, object); err != nil {
			return false, err
		}
		var service corev1.Service
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(object), &service); err != nil {
			return false, err
		}
		address, err := loadBalancerAddress(&service)
		if err != nil {
			setLocalCondition(&result.Conditions, "LoadBalancersAllocated", metav1.ConditionFalse,
				"AddressPending", err.Error())
			return false, nil
		}
		result.Addresses[service.Name] = address
	}
	setLocalCondition(&result.Conditions, "LoadBalancersAllocated", metav1.ConditionTrue,
		"AddressesAllocated", "All member addresses are allocated")
	return true, nil
}

func (r *Reconciler) reconcileSecrets(ctx context.Context, desired *plan.SitePlan,
	result *ApplyResult,
) error {
	if err := r.Secrets.Reconcile(ctx, *desired); err != nil {
		setLocalCondition(&result.Conditions, "VaultReady", metav1.ConditionFalse,
			"SecretResolutionFailed", err.Error())
		return err
	}
	setLocalCondition(&result.Conditions, "VaultReady", metav1.ConditionTrue,
		"SecretsResolved", "Required Vault references were resolved")
	if desired.Site.Role != api.SiteRoleData {
		return nil
	}
	version, err := r.setCredentialConditions(ctx, *desired, result)
	if err != nil {
		return err
	}
	desired.RuntimeCredentialVersion = version
	return nil
}

func (r *Reconciler) setCredentialConditions(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) (int64, error) {
	var active corev1.Secret
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: desired.Site.Namespace, Name: "postgres-auth",
	}, &active); err != nil {
		return 0, err
	}
	activeVersion := active.Annotations["multisite-postgres.dev/vault-version"]
	version, err := strconv.ParseInt(activeVersion, 10, 64)
	if err != nil || version < 1 {
		return 0, fmt.Errorf("postgres-auth has invalid Vault version %q", activeVersion)
	}
	setLocalCondition(&result.Conditions, "PostgresCredentialsActive", metav1.ConditionTrue,
		"VaultVersionActive", activeVersion)
	var pending corev1.Secret
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: desired.Site.Namespace, Name: "postgres-auth-pending",
	}, &pending); err == nil {
		setLocalCondition(&result.Conditions, "CredentialRotationPending", metav1.ConditionTrue,
			"VaultVersionChanged", pending.Annotations["multisite-postgres.dev/vault-version"])
	} else if !apierrors.IsNotFound(err) {
		return 0, err
	} else {
		setLocalCondition(&result.Conditions, "CredentialRotationPending", metav1.ConditionFalse,
			"ActiveVersionCurrent", activeVersion)
	}
	return version, nil
}

func (r *Reconciler) reconcileCertificates(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	certificates := r.Renderer.Certificates(desired)
	for _, object := range certificates {
		if err := r.apply(ctx, object); err != nil {
			return false, err
		}
	}
	ready, message, err := r.certificatesReady(ctx, certificates)
	if err != nil {
		return false, err
	}
	if !ready {
		setLocalCondition(&result.Conditions, "CertificatesReady", metav1.ConditionFalse,
			"IssuancePending", message)
		return false, nil
	}
	setLocalCondition(&result.Conditions, "CertificatesReady", metav1.ConditionTrue,
		"CertificatesIssued", "All workload certificates are Ready")
	etcdFingerprint, err := r.trustBundleFingerprint(ctx, desired.Site.Namespace,
		"etcd-maintenance-client-tls")
	if err != nil {
		setLocalCondition(&result.Conditions, "EtcdTLSReady", metav1.ConditionFalse,
			"TrustBundleInvalid", err.Error())
		return false, err
	}
	setLocalCondition(&result.Conditions, "EtcdTLSReady", metav1.ConditionTrue,
		"TrustBundleObserved", etcdFingerprint)
	if desired.Backup == nil {
		return true, nil
	}
	fingerprint, err := r.trustBundleFingerprint(ctx, desired.Site.Namespace,
		"pgbackrest-client-tls")
	if err != nil {
		setLocalCondition(&result.Conditions, "BackupTLSReady", metav1.ConditionFalse,
			"TrustBundleInvalid", err.Error())
		return false, err
	}
	setLocalCondition(&result.Conditions, "BackupTLSReady", metav1.ConditionTrue,
		"TrustBundleObserved", fingerprint)
	return true, nil
}

func (r *Reconciler) reconcileAddressMigration(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	job, err := r.Renderer.AddressMigrationJob(desired)
	if err != nil {
		setLocalCondition(&result.Conditions, "AddressChangeBlocked", metav1.ConditionTrue,
			"QuorumUnsafe", err.Error())
		return false, err
	}
	if job != nil {
		if err := r.apply(ctx, job); err != nil {
			return false, err
		}
		ready, message, err := r.workloadsReady(ctx, []client.Object{job})
		if err != nil {
			return false, err
		}
		if !ready {
			setLocalCondition(&result.Conditions, "AddressChangeBlocked", metav1.ConditionTrue,
				"MembershipUpdatePending", message)
			return false, nil
		}
	}
	setLocalCondition(&result.Conditions, "AddressChangeBlocked", metav1.ConditionFalse,
		"MembershipUpdated", "Address migration preconditions and etcd membership update completed")
	return true, nil
}

func (r *Reconciler) trustBundleFingerprint(ctx context.Context, namespace, secretName string) (string, error) {
	var secret corev1.Secret
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: namespace, Name: secretName,
	}, &secret); err != nil {
		return "", err
	}
	remaining := secret.Data["ca.crt"]
	var fingerprints []string
	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil {
			break
		}
		remaining = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return "", fmt.Errorf("parse pgBackRest CA certificate: %w", err)
		}
		sum := sha256.Sum256(certificate.Raw)
		fingerprints = append(fingerprints, hex.EncodeToString(sum[:]))
	}
	if len(fingerprints) == 0 {
		return "", fmt.Errorf("secret %s contains no CA certificates", secretName)
	}
	slices.Sort(fingerprints)
	return strings.Join(fingerprints, ","), nil
}

func (r *Reconciler) setDataPlaneConditions(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) error {
	if restoreExpectsPostgres(desired) {
		setLocalCondition(&result.Conditions, "PatroniReady", metav1.ConditionTrue,
			"AllMembersHealthy", "All Patroni member readiness checks are passing")
		if err := r.observeTopology(ctx, desired, result); err != nil {
			return err
		}
		if err := r.reconcileCredentialCatalog(ctx, desired, result); err != nil {
			return err
		}
		if err := r.reconcileUpgradeAction(ctx, desired, result); err != nil {
			return err
		}
		if desired.TDE.Enabled {
			setLocalCondition(&result.Conditions, "TDEVerified", metav1.ConditionTrue,
				"DatabaseAuditPassed",
				"Every local member resolved its pg_tde key and reported no unencrypted user relations")
		}
	} else if desired.Site.Role == api.SiteRoleData && desired.MajorUpgrade != nil {
		setLocalCondition(&result.Conditions, "MajorUpgradeWaiting", metav1.ConditionTrue,
			"PostgreSQLIntentionallyStopped", "PostgreSQL remains stopped for the active major-upgrade phase")
	} else if desired.Site.Role == api.SiteRoleData {
		setLocalCondition(&result.Conditions, "RestoreWaiting", metav1.ConditionTrue,
			"SeedRecoveryInProgress", "PostgreSQL remains stopped until the restored seed member is promoted")
	}
	return nil
}

func (r *Reconciler) reconcileUpgradeAction(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) error {
	if desired.Upgrade == nil || desired.Upgrade.Phase != plan.UpgradePhaseSwitchover ||
		!memberBelongsToSite(desired.Upgrade.FromPrimary, desired.Site.Name) {
		return nil
	}
	if result.Primary == desired.Upgrade.Candidate {
		setLocalCondition(&result.Conditions, "SwitchoverCompleted", metav1.ConditionTrue,
			"CandidatePromoted", "Patroni reports the upgraded candidate as primary")
		return nil
	}
	if result.Primary != desired.Upgrade.FromPrimary {
		return fmt.Errorf("patroni primary changed unexpectedly from %s to %s",
			desired.Upgrade.FromPrimary, result.Primary)
	}
	if r.Topology == nil {
		return fmt.Errorf("patroni topology client is required for switchover")
	}
	if err := r.Topology.Switchover(ctx, desired,
		desired.Upgrade.FromPrimary, desired.Upgrade.Candidate); err != nil {
		return fmt.Errorf("request Patroni switchover: %w", err)
	}
	setLocalCondition(&result.Conditions, "SwitchoverCompleted", metav1.ConditionFalse,
		"RequestAccepted", "Patroni accepted the controlled switchover request")
	return nil
}

func memberBelongsToSite(member, site string) bool {
	return strings.HasPrefix(member, "postgres-"+site+"-")
}

func restoreExpectsPostgres(desired plan.SitePlan) bool {
	if desired.Site.Role != api.SiteRoleData {
		return false
	}
	if desired.MajorUpgrade != nil {
		switch desired.MajorUpgrade.Phase {
		case plan.MajorUpgradePhaseStop, plan.MajorUpgradePhaseSnapshot,
			plan.MajorUpgradePhaseUpgradePrimary, plan.MajorUpgradePhaseStanzaUpgrade,
			plan.MajorUpgradePhaseRollback:
			return false
		case plan.MajorUpgradePhaseStartPrimary, plan.MajorUpgradePhaseRestoreWrites:
			return memberBelongsToSite(desired.MajorUpgrade.Primary, desired.Site.Name)
		}
	}
	return desired.Restore == nil || desired.Restore.Phase != plan.RestorePhaseSeed ||
		desired.Site.Name == desired.Restore.SeedSite
}

func (r *Reconciler) observeTopology(ctx context.Context, desired plan.SitePlan, result *ApplyResult) error {
	if r.Topology == nil {
		return nil
	}
	topology, err := r.Topology.Observe(ctx, desired)
	if err != nil {
		setLocalCondition(&result.Conditions, "TopologyReady", metav1.ConditionFalse,
			"ObservationFailed", err.Error())
		return err
	}
	result.Primary = topology.Primary
	result.SynchronousStandbys = topology.SynchronousStandbys
	setLocalCondition(&result.Conditions, "TopologyReady", metav1.ConditionTrue,
		"PatroniObserved", "Patroni topology is observable")
	return nil
}

func (r *Reconciler) deleteInstance(ctx context.Context, desired plan.SitePlan) (ApplyResult, error) {
	result := ApplyResult{Phase: "Deleting", Addresses: map[string]string{}}
	var namespace corev1.Namespace
	if err := r.Client.Get(ctx, client.ObjectKey{Name: desired.Site.Namespace}, &namespace); apierrors.IsNotFound(err) {
		result.Phase = "Deleted"
		setLocalCondition(&result.Conditions, "Deleted", metav1.ConditionTrue,
			"NamespaceAbsent", "The target namespace no longer exists")
		return result, nil
	} else if err != nil {
		return result, err
	}
	if err := EnsureNamespace(ctx, r.Client, desired.Site.Namespace, r.HubDomain, r.SiteUID,
		desired.InstanceUID, true); err != nil {
		return result, err
	}
	if desired.Deletion.Policy == api.DeletionPolicyDelete {
		if err := r.Client.Delete(ctx, &namespace); err != nil && !apierrors.IsNotFound(err) {
			return result, err
		}
		setLocalCondition(&result.Conditions, "Deleted", metav1.ConditionFalse,
			"NamespaceTerminating", "Waiting for the target namespace to terminate")
		return result, nil
	}
	if err := r.deleteManagedObjects(ctx, desired, ""); err != nil {
		return result, err
	}
	var claims corev1.PersistentVolumeClaimList
	if err := r.Client.List(ctx, &claims, client.InNamespace(desired.Site.Namespace), client.MatchingLabels{
		"app.kubernetes.io/managed-by":        "mspsql-agent",
		"multisite-postgres.dev/instance-uid": desired.InstanceUID,
	}); err != nil {
		return result, err
	}
	for i := range claims.Items {
		claim := &claims.Items[i]
		base := claim.DeepCopy()
		if claim.Labels == nil {
			claim.Labels = map[string]string{}
		}
		claim.Labels["multisite-postgres.dev/retained-instance-uid"] = desired.InstanceUID
		if err := r.Client.Patch(ctx, claim, client.MergeFrom(base)); err != nil {
			return result, err
		}
	}
	result.Phase = "Deleted"
	setLocalCondition(&result.Conditions, "Deleted", metav1.ConditionTrue,
		"WorkloadsRemoved", "Active workloads were removed and persistent data was retained")
	return result, nil
}

func (r *Reconciler) apply(ctx context.Context, object client.Object) error {
	encoded, err := encodeApplyObject(object, r.Client.Scheme())
	if err != nil {
		return err
	}
	if err := r.Client.Patch(ctx, object, client.RawPatch(types.ApplyPatchType, encoded),
		client.FieldOwner("mspsql-agent")); err != nil {
		return fmt.Errorf("apply %T %s/%s: %w",
			object, object.GetNamespace(), object.GetName(), err)
	}
	return nil
}

func encodeApplyObject(object client.Object, scheme *runtime.Scheme) ([]byte, error) {
	gvk, err := apiutil.GVKForObject(object, scheme)
	if err != nil {
		return nil, fmt.Errorf("resolve type for %T %s/%s: %w",
			object, object.GetNamespace(), object.GetName(), err)
	}
	object.GetObjectKind().SetGroupVersionKind(gvk)
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("marshal %T %s/%s: %w",
			object, object.GetNamespace(), object.GetName(), err)
	}
	return encoded, nil
}

func (r *Reconciler) pruneStaleObjects(ctx context.Context, desired plan.SitePlan) error {
	return r.deleteManagedObjects(ctx, desired, fmt.Sprintf("%d", desired.Revision))
}

func (r *Reconciler) deleteManagedObjects(ctx context.Context, desired plan.SitePlan, keepRevision string) error {
	certificates := &unstructured.UnstructuredList{}
	certificates.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "CertificateList",
	})
	lists := []client.ObjectList{
		&corev1.ServiceList{},
		&corev1.ConfigMapList{},
		&appsv1.StatefulSetList{},
		&appsv1.DeploymentList{},
		&batchv1.JobList{},
		&batchv1.CronJobList{},
		certificates,
	}
	labels := client.MatchingLabels{
		"app.kubernetes.io/managed-by":        "mspsql-agent",
		"multisite-postgres.dev/instance-uid": desired.InstanceUID,
	}
	for _, list := range lists {
		if err := r.Client.List(ctx, list, client.InNamespace(desired.Site.Namespace), labels); err != nil {
			return err
		}
		objects, err := meta.ExtractList(list)
		if err != nil {
			return err
		}
		for _, object := range objects {
			managed, ok := object.(client.Object)
			if !ok || keepRevision != "" &&
				managed.GetLabels()["multisite-postgres.dev/desired-revision"] == keepRevision {
				continue
			}
			if err := r.Client.Delete(ctx, managed); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (r *Reconciler) certificatesReady(ctx context.Context, certificates []client.Object) (bool, string, error) {
	for _, expected := range certificates {
		certificate, ok := expected.(*unstructured.Unstructured)
		if !ok {
			return false, "", fmt.Errorf("certificate renderer returned %T", expected)
		}
		observed := &unstructured.Unstructured{}
		observed.SetGroupVersionKind(certificate.GroupVersionKind())
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(certificate), observed); err != nil {
			return false, "", err
		}
		conditions, found, err := unstructured.NestedSlice(observed.Object, "status", "conditions")
		if err != nil {
			return false, "", err
		}
		isReady := false
		for _, raw := range conditions {
			condition, conditionOK := raw.(map[string]any)
			if conditionOK && condition["type"] == "Ready" && condition["status"] == "True" &&
				integerValue(condition["observedGeneration"]) >= observed.GetGeneration() {
				isReady = true
				break
			}
		}
		if !found {
			return false, fmt.Sprintf("Certificate %s has not reported status", observed.GetName()), nil
		}
		if !isReady {
			return false, fmt.Sprintf("Certificate %s is not Ready for generation %d",
				observed.GetName(), observed.GetGeneration()), nil
		}
		secretName, found, err := unstructured.NestedString(observed.Object, "spec", "secretName")
		if err != nil || !found || secretName == "" {
			return false, fmt.Sprintf("Certificate %s has no output Secret", observed.GetName()), err
		}
		var secret corev1.Secret
		if err := r.Client.Get(ctx, client.ObjectKey{
			Namespace: observed.GetNamespace(), Name: secretName,
		}, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				return false, fmt.Sprintf("Certificate %s output Secret is absent", observed.GetName()), nil
			}
			return false, "", err
		}
		if err := validateIssuedCertificate(observed, &secret, time.Now()); err != nil {
			return false, fmt.Sprintf("Certificate %s is unusable: %v", observed.GetName(), err), nil
		}
	}
	return true, "", nil
}

func integerValue(value any) int64 {
	switch value := value.(type) {
	case int64:
		return value
	case int32:
		return int64(value)
	case int:
		return int64(value)
	case float64:
		return int64(value)
	default:
		return 0
	}
}

func validateIssuedCertificate(expected *unstructured.Unstructured, secret *corev1.Secret,
	now time.Time,
) error {
	leafBlock, _ := pem.Decode(secret.Data[corev1.TLSCertKey])
	if leafBlock == nil || leafBlock.Type != "CERTIFICATE" {
		return fmt.Errorf("secret %s has no leaf certificate", secret.Name)
	}
	leaf, err := x509.ParseCertificate(leafBlock.Bytes)
	if err != nil {
		return fmt.Errorf("parse leaf certificate: %w", err)
	}
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return fmt.Errorf("leaf certificate is outside its validity period")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data["ca.crt"]) {
		return fmt.Errorf("secret %s has no valid CA bundle", secret.Name)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}, CurrentTime: now,
	}); err != nil {
		return fmt.Errorf("verify CA chain: %w", err)
	}
	dnsNames, _, err := unstructured.NestedStringSlice(expected.Object, "spec", "dnsNames")
	if err != nil {
		return err
	}
	for _, name := range dnsNames {
		if err := leaf.VerifyHostname(name); err != nil {
			return fmt.Errorf("required DNS SAN %q is absent", name)
		}
	}
	ipAddresses, _, err := unstructured.NestedStringSlice(expected.Object, "spec", "ipAddresses")
	if err != nil {
		return err
	}
	for _, address := range ipAddresses {
		ip := net.ParseIP(address)
		if ip == nil || !slices.ContainsFunc(leaf.IPAddresses, ip.Equal) {
			return fmt.Errorf("required IP SAN %q is absent", address)
		}
	}
	usages, _, err := unstructured.NestedStringSlice(expected.Object, "spec", "usages")
	if err != nil {
		return err
	}
	for _, usage := range usages {
		switch usage {
		case "digital signature":
			if leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
				return fmt.Errorf("digital signature key usage is absent")
			}
		case "key encipherment":
			if leaf.KeyUsage&x509.KeyUsageKeyEncipherment == 0 {
				return fmt.Errorf("key encipherment usage is absent")
			}
		case "server auth":
			if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
				return fmt.Errorf("server auth usage is absent")
			}
		case "client auth":
			if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
				return fmt.Errorf("client auth usage is absent")
			}
		}
	}
	return nil
}

func (r *Reconciler) workloadsReady(ctx context.Context, objects []client.Object) (bool, string, error) {
	for _, expected := range objects {
		switch expected := expected.(type) {
		case *appsv1.StatefulSet:
			var observed appsv1.StatefulSet
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(expected), &observed); err != nil {
				return false, "", err
			}
			replicas := int32(1)
			if expected.Spec.Replicas != nil {
				replicas = *expected.Spec.Replicas
			}
			if observed.Status.ObservedGeneration < observed.Generation ||
				observed.Status.ReadyReplicas != replicas ||
				observed.Status.UpdatedReplicas != replicas ||
				observed.Status.CurrentRevision != observed.Status.UpdateRevision {
				return false, fmt.Sprintf("StatefulSet %s has %d/%d Ready replicas",
					observed.Name, observed.Status.ReadyReplicas, replicas), nil
			}
		case *appsv1.Deployment:
			var observed appsv1.Deployment
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(expected), &observed); err != nil {
				return false, "", err
			}
			replicas := int32(1)
			if expected.Spec.Replicas != nil {
				replicas = *expected.Spec.Replicas
			}
			if observed.Status.ObservedGeneration < observed.Generation ||
				observed.Status.AvailableReplicas != replicas ||
				observed.Status.UpdatedReplicas != replicas {
				return false, fmt.Sprintf("Deployment %s has %d/%d Available replicas",
					observed.Name, observed.Status.AvailableReplicas, replicas), nil
			}
		case *batchv1.Job:
			var observed batchv1.Job
			if err := r.Client.Get(ctx, client.ObjectKeyFromObject(expected), &observed); err != nil {
				return false, "", err
			}
			complete := false
			for _, condition := range observed.Status.Conditions {
				if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
					complete = true
					break
				}
				if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
					return false, fmt.Sprintf("Job %s failed: %s", observed.Name, condition.Message), nil
				}
			}
			if complete {
				continue
			}
			return false, fmt.Sprintf("Job %s has not completed", observed.Name), nil
		}
	}
	return true, "", nil
}

func loadBalancerAddress(service *corev1.Service) (string, error) {
	if len(service.Status.LoadBalancer.Ingress) != 1 {
		return "", fmt.Errorf("service %s must have exactly one ingress address", service.Name)
	}
	ingress := service.Status.LoadBalancer.Ingress[0]
	if ingress.IP != "" {
		return ingress.IP, nil
	}
	if ingress.Hostname != "" {
		return ingress.Hostname, nil
	}
	return "", fmt.Errorf("service %s ingress address is empty", service.Name)
}

func mergeAddresses(first, second map[string]string) map[string]string {
	merged := make(map[string]string, len(first)+len(second))
	maps.Copy(merged, first)
	maps.Copy(merged, second)
	return merged
}

func fillMissingAddresses(planned, observed map[string]string) map[string]string {
	merged := make(map[string]string, len(planned)+len(observed))
	maps.Copy(merged, planned)
	for member, address := range observed {
		if merged[member] == "" {
			merged[member] = address
		}
	}
	return merged
}

func setLocalCondition(conditions *[]metav1.Condition, conditionType string,
	status metav1.ConditionStatus, reason, message string,
) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: conditionType, Status: status, Reason: reason, Message: message,
	})
}
