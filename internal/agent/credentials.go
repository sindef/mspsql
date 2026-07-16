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
	"fmt"
	"slices"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sindef/mspsql/internal/plan"
)

func (r *Reconciler) prepareCredentialRotation(ctx context.Context, desired *plan.SitePlan,
	result *ApplyResult,
) (bool, error) {
	rotation := desired.CredentialRotation
	if rotation == nil {
		return true, nil
	}
	switch rotation.Phase {
	case plan.CredentialRotationPhaseFinalize:
		if err := r.deleteCredentialRotationSecrets(ctx, desired.Site.Namespace); err != nil {
			return false, err
		}
		setLocalCondition(&result.Conditions, "CredentialRotationPending", metav1.ConditionFalse,
			"RotationCompleted", strconv.FormatInt(rotation.Version, 10))
		return true, nil
	case plan.CredentialRotationPhaseMember:
		if !memberBelongsToSite(rotation.TargetMember, desired.Site.Name) {
			return true, nil
		}
		result.Phase = "VerifyingCredentials"
		job := r.Renderer.CredentialVerificationJob(*desired)
		if err := r.apply(ctx, job); err != nil {
			return false, err
		}
		ready, message, err := r.workloadsReady(ctx, []client.Object{job})
		if err != nil {
			return false, err
		}
		if !ready {
			setLocalCondition(&result.Conditions, "CredentialRotationBlocked", metav1.ConditionTrue,
				"CatalogReplicationPending", message)
			return false, nil
		}
		if err := r.promotePendingCredentials(ctx, desired.Site.Namespace); err != nil {
			return false, err
		}
		setLocalCondition(&result.Conditions, "CredentialRotationBlocked", metav1.ConditionFalse,
			"CredentialVerified", "The staged credential authenticates against the target member")
	}
	return true, nil
}

func (r *Reconciler) reconcileCredentialCatalog(ctx context.Context, desired plan.SitePlan,
	result *ApplyResult,
) error {
	rotation := desired.CredentialRotation
	if rotation == nil ||
		(rotation.Phase != plan.CredentialRotationPhaseCatalog &&
			rotation.Phase != plan.CredentialRotationPhaseRevoke) ||
		!memberBelongsToSite(result.Primary, desired.Site.Name) {
		return nil
	}
	job := r.Renderer.CredentialCatalogJob(desired, result.Primary)
	if rotation.Phase == plan.CredentialRotationPhaseRevoke {
		job = r.Renderer.CredentialRevokeJob(desired, result.Primary)
	}
	if err := r.apply(ctx, job); err != nil {
		return err
	}
	ready, message, err := r.workloadsReady(ctx, []client.Object{job})
	if err != nil {
		return err
	}
	if !ready {
		setLocalCondition(&result.Conditions, "CredentialRotationBlocked", metav1.ConditionTrue,
			"CatalogUpdatePending", message)
		result.Phase = "RotatingCredentials"
		return nil
	}
	conditionType := "CredentialCatalogUpdated"
	reason := "RolesCreated"
	if rotation.Phase == plan.CredentialRotationPhaseRevoke {
		conditionType = "PreviousCredentialsRevoked"
		reason = "PreviousRolesDisabled"
	}
	setLocalCondition(&result.Conditions, conditionType, metav1.ConditionTrue,
		reason, strconv.FormatInt(rotation.Version, 10))
	return nil
}

func (r *Reconciler) setCredentialMemberCondition(desired plan.SitePlan, result *ApplyResult) {
	rotation := desired.CredentialRotation
	if rotation == nil || rotation.Phase != plan.CredentialRotationPhaseMember ||
		!memberBelongsToSite(rotation.TargetMember, desired.Site.Name) {
		return
	}
	members := slices.Clone(rotation.UpdatedMembers)
	members = append(members, rotation.TargetMember)
	slices.Sort(members)
	members = slices.Compact(members)
	setLocalCondition(&result.Conditions, "CredentialMembersUpdated", metav1.ConditionTrue,
		"MemberReloaded", strconv.FormatInt(rotation.Version, 10)+":"+strings.Join(members, ","))
}

func (r *Reconciler) promotePendingCredentials(ctx context.Context, namespace string) error {
	var pending corev1.Secret
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: namespace, Name: "postgres-auth-pending",
	}, &pending); err != nil {
		return err
	}
	var active corev1.Secret
	if err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: namespace, Name: "postgres-auth",
	}, &active); err != nil {
		return err
	}
	var previous corev1.Secret
	previousKey := client.ObjectKey{Namespace: namespace, Name: "postgres-auth-previous"}
	if err := r.Client.Get(ctx, previousKey, &previous); apierrors.IsNotFound(err) {
		previous = *active.DeepCopy()
		previous.ResourceVersion = ""
		previous.UID = ""
		previous.Name = previousKey.Name
		if err := r.Client.Create(ctx, &previous); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	active.Data = pending.Data
	if active.Annotations == nil {
		active.Annotations = map[string]string{}
	}
	active.Annotations["multisite-postgres.dev/vault-version"] =
		pending.Annotations["multisite-postgres.dev/vault-version"]
	return r.Client.Update(ctx, &active)
}

func (r *Reconciler) deleteCredentialRotationSecrets(ctx context.Context, namespace string) error {
	for _, name := range []string{"postgres-auth-pending", "postgres-auth-previous"} {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
		if err := r.Client.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r Renderer) CredentialCatalogJob(desired plan.SitePlan, primary string) *batchv1.Job {
	script := `set -euo pipefail
psql -X -h "$PRIMARY" -d postgres -v ON_ERROR_STOP=1 <<'SQL'
\getenv new_superuser NEW_SUPERUSER_PASSWORD
\getenv new_replication NEW_REPLICATION_PASSWORD
\getenv new_superuser_name NEW_SUPERUSER_USERNAME
\getenv new_replication_name NEW_REPLICATION_USERNAME
\getenv old_superuser_name OLD_SUPERUSER_USERNAME
\getenv old_replication_name OLD_REPLICATION_USERNAME
SELECT 1 / (
  :'new_superuser_name' <> :'old_superuser_name'
  AND :'new_replication_name' <> :'old_replication_name'
  AND :'new_superuser_name' <> :'new_replication_name'
)::int;
SELECT format('CREATE ROLE %I LOGIN', :'new_superuser_name')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'new_superuser_name') \gexec
SELECT format('ALTER ROLE %I LOGIN SUPERUSER PASSWORD %L', :'new_superuser_name', :'new_superuser') \gexec
SELECT format('CREATE ROLE %I LOGIN', :'new_replication_name')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'new_replication_name') \gexec
SELECT format('ALTER ROLE %I LOGIN REPLICATION PASSWORD %L', :'new_replication_name', :'new_replication') \gexec
SQL
`
	return credentialJob(desired, "credential-catalog-"+strconv.FormatInt(
		desired.CredentialRotation.Version, 10), script, []corev1.EnvVar{
		{Name: "PRIMARY", Value: primary + "." + desired.Site.Namespace + ".svc"},
		{Name: "PGUSER", ValueFrom: secretKeySelector("postgres-auth", "superuser-username")},
		{Name: "PGPASSWORD", ValueFrom: secretKeySelector("postgres-auth", "superuser-password")},
		{Name: "OLD_SUPERUSER_USERNAME", ValueFrom: secretKeySelector(
			"postgres-auth", "superuser-username")},
		{Name: "OLD_REPLICATION_USERNAME", ValueFrom: secretKeySelector(
			"postgres-auth", "replication-username")},
		{Name: "NEW_SUPERUSER_USERNAME", ValueFrom: secretKeySelector(
			"postgres-auth-pending", "superuser-username")},
		{Name: "NEW_SUPERUSER_PASSWORD", ValueFrom: secretKeySelector(
			"postgres-auth-pending", "superuser-password")},
		{Name: "NEW_REPLICATION_USERNAME", ValueFrom: secretKeySelector(
			"postgres-auth-pending", "replication-username")},
		{Name: "NEW_REPLICATION_PASSWORD", ValueFrom: secretKeySelector(
			"postgres-auth-pending", "replication-password")},
	})
}

func (r Renderer) CredentialVerificationJob(desired plan.SitePlan) *batchv1.Job {
	target := desired.CredentialRotation.TargetMember
	script := `set -euo pipefail
psql -X -h "$TARGET" -d postgres -v ON_ERROR_STOP=1 -Atqc 'SELECT 1' |
  grep -qx 1
`
	return credentialJob(desired, "credential-verify-"+operationHash(
		fmt.Sprintf("%d/%s", desired.CredentialRotation.Version, target)), script, []corev1.EnvVar{
		{Name: "TARGET", Value: target + "." + desired.Site.Namespace + ".svc"},
		{Name: "PGUSER", ValueFrom: secretKeySelector(
			"postgres-auth-pending", "superuser-username")},
		{Name: "PGPASSWORD", ValueFrom: secretKeySelector(
			"postgres-auth-pending", "superuser-password")},
	})
}

func (r Renderer) CredentialRevokeJob(desired plan.SitePlan, primary string) *batchv1.Job {
	script := `set -euo pipefail
psql -X -h "$PRIMARY" -d postgres -v ON_ERROR_STOP=1 <<'SQL'
\getenv old_superuser OLD_SUPERUSER_USERNAME
\getenv old_replication OLD_REPLICATION_USERNAME
SELECT format('ALTER ROLE %I NOLOGIN', :'old_superuser') \gexec
SELECT format('ALTER ROLE %I NOLOGIN', :'old_replication') \gexec
SQL
`
	return credentialJob(desired, "credential-revoke-"+strconv.FormatInt(
		desired.CredentialRotation.Version, 10), script, []corev1.EnvVar{
		{Name: "PRIMARY", Value: primary + "." + desired.Site.Namespace + ".svc"},
		{Name: "PGUSER", ValueFrom: secretKeySelector("postgres-auth", "superuser-username")},
		{Name: "PGPASSWORD", ValueFrom: secretKeySelector("postgres-auth", "superuser-password")},
		{Name: "OLD_SUPERUSER_USERNAME", ValueFrom: secretKeySelector(
			"postgres-auth-previous", "superuser-username")},
		{Name: "OLD_REPLICATION_USERNAME", ValueFrom: secretKeySelector(
			"postgres-auth-previous", "replication-username")},
	})
}

func credentialJob(desired plan.SitePlan, name, script string, environment []corev1.EnvVar) *batchv1.Job {
	backoff := int32(3)
	deadline := int64(600)
	environment = append(environment,
		corev1.EnvVar{Name: "PGSSLMODE", Value: "verify-full"},
		corev1.EnvVar{Name: "PGSSLROOTCERT", Value: "/tls/ca.crt"})
	labels := resourceLabels(desired)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: name, Labels: labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: stableWorkloadLabels(labels)},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           workloadServiceAccount,
					AutomountServiceAccountToken: ptr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), FSGroup: ptr(int64(26)),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: "credentials", Image: desired.Postgres.Image,
						Command: []string{"/bin/bash", "-ec", script},
						Env:     environment, SecurityContext: restrictedContainer(),
						VolumeMounts: []corev1.VolumeMount{{
							Name: "tls", MountPath: "/tls", ReadOnly: true,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "tls", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: "postgres-" + desired.Site.Name + "-0-tls",
							},
						},
					}},
				},
			},
		},
	}
}
