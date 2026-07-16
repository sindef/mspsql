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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/directive"
	"github.com/sindef/mspsql/internal/plan"
	"github.com/sindef/mspsql/internal/vault"
)

type DirectiveExecutor struct {
	Client  client.Client
	Cache   *Cache
	Secrets *SecretMaterializer
}

func (e *DirectiveExecutor) Execute(ctx context.Context, payload directive.Payload) ([]metav1.Condition, error) {
	desired, err := e.Cache.Load(ctx, payload.InstanceUID)
	if err != nil {
		return nil, fmt.Errorf("load accepted site plan: %w", err)
	}
	if payload.Primary == "" {
		return nil, errors.New("directive has no observed primary")
	}
	if _, found := desired.MemberAddresses[payload.Primary]; !found {
		return nil, fmt.Errorf("primary %q is not in the accepted site plan", payload.Primary)
	}
	switch payload.Type {
	case "Database":
		return e.executeDatabase(ctx, desired, payload)
	case "User":
		return e.executeUser(ctx, desired, payload)
	case "Backup":
		return e.executeBackup(ctx, desired, payload)
	default:
		return nil, fmt.Errorf("unsupported directive type %q", payload.Type)
	}
}

func (e *DirectiveExecutor) executeBackup(ctx context.Context, desired plan.SitePlan,
	payload directive.Payload,
) ([]metav1.Condition, error) {
	if desired.Backup == nil {
		return nil, errors.New("backup directive targets an instance without a repository")
	}
	if payload.BackupSource == "" || payload.Primary == "" {
		return nil, errors.New("backup directive has no selected source or primary")
	}
	if payload.BackupType != "full" && payload.BackupType != "diff" && payload.BackupType != "incr" {
		return nil, fmt.Errorf("unsupported backup type %q", payload.BackupType)
	}
	config, err := backupCoordinatorConfig(desired, payload)
	if err != nil {
		return nil, err
	}
	name := "mspsql-backup-" + operationHash(payload.OperationUID)
	key := client.ObjectKey{Namespace: desired.Site.Namespace, Name: name}
	var observed batchv1.Job
	if err := e.Client.Get(ctx, key, &observed); err == nil {
		conditions, waitErr := waitForJob(ctx, e.Client, key, "BackupCompleted",
			"pgBackRest completed and verified repository metadata")
		if waitErr != nil {
			return conditions, waitErr
		}
		metadata, metadataErr := backupMetadata(ctx, e.Client, desired.Site.Namespace, name)
		return append(conditions, metadata...), metadataErr
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}
	job := backupJob(desired, payload, name, config)
	if err := e.Client.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	conditions, waitErr := waitForJob(ctx, e.Client, key, "BackupCompleted",
		"pgBackRest completed and verified repository metadata")
	if waitErr != nil {
		return conditions, waitErr
	}
	metadata, metadataErr := backupMetadata(ctx, e.Client, desired.Site.Namespace, name)
	return append(conditions, metadata...), metadataErr
}

func backupCoordinatorConfig(desired plan.SitePlan, payload directive.Payload) (string, error) {
	repository := desired.Backup.Repository
	if repository.Region == "" {
		repository.Region = "us-east-1"
	}
	if repository.URIStyle == "" {
		repository.URIStyle = "host"
	}
	endpoint := repository.Endpoint
	if endpoint == "" {
		endpoint = "https://s3." + repository.Region + ".amazonaws.com"
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return "", errors.New("backup repository endpoint must be an absolute HTTPS URL")
	}
	primaryAddress, primaryFound := desired.MemberAddresses[payload.Primary]
	sourceAddress, sourceFound := desired.MemberAddresses[payload.BackupSource]
	if !primaryFound || !sourceFound {
		return "", errors.New("backup source addresses are absent from the accepted plan")
	}
	for _, value := range []string{
		repository.Bucket, repository.Prefix, repository.Region, repository.URIStyle,
		primaryAddress, sourceAddress,
	} {
		if strings.ContainsAny(value, "\r\n") {
			return "", errors.New("backup configuration values must be single-line")
		}
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	caConfig := ""
	if repository.CABundleSecretRef != nil {
		caConfig = "repo1-storage-ca-file=/repository/ca.crt\n"
	}
	retentionDays := int64(7)
	if desired.Backup.Retention.Duration.Duration > 0 {
		retentionDays = int64((desired.Backup.Retention.Duration.Duration + 24*time.Hour - 1) / (24 * time.Hour))
	}
	stanza := "mspsql-" + desired.InstanceUID
	hostConfig := pgBackRestHostConfig(1, primaryAddress)
	backupStandby := "n"
	if payload.BackupSource != payload.Primary {
		hostConfig += pgBackRestHostConfig(2, sourceAddress)
		backupStandby = "y"
	}
	return fmt.Sprintf(`[global]
repo1-type=s3
repo1-path=/%s
repo1-s3-bucket=%s
repo1-s3-endpoint=%s
repo1-s3-region=%s
repo1-s3-uri-style=%s
repo1-storage-port=%s
repo1-storage-verify-tls=y
%srepo1-s3-key=${S3_ACCESS_KEY}
repo1-s3-key-secret=${S3_SECRET_KEY}
repo1-cipher-type=aes-256-cbc
repo1-cipher-pass=${REPO_CIPHER_PASSPHRASE}
repo1-retention-full-type=time
repo1-retention-full=%d
process-max=4
start-fast=y
backup-standby=%s

[%s]
%s`, strings.Trim(repository.Prefix, "/"), repository.Bucket, parsed.Hostname(), repository.Region,
		repository.URIStyle, port, caConfig, retentionDays, backupStandby, stanza, hostConfig), nil
}

func pgBackRestHostConfig(index int, address string) string {
	return fmt.Sprintf(`pg%d-host=%s
pg%d-host-type=tls
pg%d-host-port=8432
pg%d-host-user=postgres
pg%d-host-ca-file=/tls/ca.crt
pg%d-host-cert-file=/tls/tls.crt
pg%d-host-key-file=/tls/tls.key
pg%d-path=/var/lib/postgresql/data
`, index, address, index, index, index, index, index, index, index)
}

func backupJob(desired plan.SitePlan, payload directive.Payload, name, config string) *batchv1.Job {
	backoff := int32(1)
	deadline := int64(24 * 60 * 60)
	ttl := int32(7 * 24 * 60 * 60)
	stanza := "mspsql-" + desired.InstanceUID
	command := fmt.Sprintf(`umask 077
export S3_ACCESS_KEY="$(cat /repository/s3-access-key)"
export S3_SECRET_KEY="$(cat /repository/s3-secret-key)"
export REPO_CIPHER_PASSPHRASE="$(cat /repository/repo-cipher-passphrase)"
envsubst > /tmp/pgbackrest.conf <<'CONFIG'
%s
CONFIG
pgbackrest --config=/tmp/pgbackrest.conf --stanza=%s stanza-create
pgbackrest --config=/tmp/pgbackrest.conf --stanza=%s check
pgbackrest --config=/tmp/pgbackrest.conf --stanza=%s --type=%s backup
pgbackrest --config=/tmp/pgbackrest.conf --stanza=%s check
pgbackrest --config=/tmp/pgbackrest.conf --stanza=%s --output=json info > /tmp/pgbackrest-info.json
python3 - <<'PY' > /dev/termination-log
import json
with open('/tmp/pgbackrest-info.json', encoding='utf-8') as stream:
    stanza = json.load(stream)[0]
backups = stanza.get('backup', [])
archives = stanza.get('archive', [])
if not backups or not archives or not archives[-1].get('max'):
    raise SystemExit('pgBackRest repository metadata is incomplete')
print(json.dumps({
    'backupSet': backups[-1]['label'],
    'completedAt': backups[-1]['timestamp']['stop'],
    'recoveryWindowStart': backups[0]['timestamp']['start'],
    'archiveMin': archives[-1].get('min', ''),
    'archiveMax': archives[-1]['max'],
}, separators=(',', ':')))
PY
`, config, stanza, stanza, stanza, payload.BackupType, stanza, stanza)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: name,
			Labels: map[string]string{
				"multisite-postgres.dev/instance-uid":  payload.InstanceUID,
				"multisite-postgres.dev/operation-uid": payload.OperationUID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline, TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					"multisite-postgres.dev/operation-uid": payload.OperationUID,
				}},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					ServiceAccountName:            workloadServiceAccount,
					AutomountServiceAccountToken:  ptr(false),
					TerminationGracePeriodSeconds: ptr(int64(30)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name: "pgbackrest", Image: desired.Postgres.Image,
						Command:         []string{"/bin/bash", "-ec", command},
						SecurityContext: restrictedContainer(),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "repository", MountPath: "/repository", ReadOnly: true},
							{Name: "tls", MountPath: "/tls", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "repository", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: "pgbackrest-repository"},
						}},
						{Name: "tls", VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: "pgbackrest-client-tls"},
						}},
					},
				},
			},
		},
	}
}

func backupMetadata(ctx context.Context, kube client.Client, namespace, jobName string,
) ([]metav1.Condition, error) {
	var pods corev1.PodList
	if err := kube.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
		"job-name": jobName,
	}); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		for _, container := range pods.Items[i].Status.ContainerStatuses {
			if container.Name != "pgbackrest" || container.State.Terminated == nil {
				continue
			}
			var metadata struct {
				BackupSet           string `json:"backupSet"`
				CompletedAt         int64  `json:"completedAt"`
				RecoveryWindowStart int64  `json:"recoveryWindowStart"`
				ArchiveMin          string `json:"archiveMin"`
				ArchiveMax          string `json:"archiveMax"`
			}
			if err := json.Unmarshal([]byte(container.State.Terminated.Message), &metadata); err != nil {
				return nil, fmt.Errorf("decode pgBackRest repository metadata: %w", err)
			}
			if metadata.BackupSet == "" || metadata.CompletedAt <= 0 ||
				metadata.RecoveryWindowStart <= 0 || metadata.ArchiveMax == "" {
				return nil, errors.New("pgBackRest repository metadata is incomplete")
			}
			now := metav1.Now()
			return []metav1.Condition{
				{
					Type: "BackupSet", Status: metav1.ConditionTrue, Reason: "RepositoryVerified",
					Message: metadata.BackupSet, LastTransitionTime: now,
				},
				{
					Type: "BackupCompletedAt", Status: metav1.ConditionTrue, Reason: "RepositoryVerified",
					Message:            time.Unix(metadata.CompletedAt, 0).UTC().Format(time.RFC3339),
					LastTransitionTime: now,
				},
				{
					Type: "RecoveryWindowStart", Status: metav1.ConditionTrue, Reason: "RepositoryVerified",
					Message:            time.Unix(metadata.RecoveryWindowStart, 0).UTC().Format(time.RFC3339),
					LastTransitionTime: now,
				},
				{
					Type: "WALArchive", Status: metav1.ConditionTrue, Reason: "RepositoryVerified",
					Message: metadata.ArchiveMin + "/" + metadata.ArchiveMax, LastTransitionTime: now,
				},
			}, nil
		}
	}
	return nil, errors.New("pgBackRest Job has no terminated coordinator status")
}

func (e *DirectiveExecutor) executeDatabase(ctx context.Context, desired plan.SitePlan,
	payload directive.Payload,
) ([]metav1.Condition, error) {
	var spec api.PostgresDatabaseSpec
	if err := json.Unmarshal(payload.Spec, &spec); err != nil {
		return nil, fmt.Errorf("decode database directive: %w", err)
	}
	sql, err := databaseSQL(spec, desired.TDE.Enabled, payload.Deleting)
	if err != nil {
		return nil, err
	}
	return e.runSQLJob(ctx, desired, payload, sql, nil)
}

func (e *DirectiveExecutor) executeUser(ctx context.Context, desired plan.SitePlan,
	payload directive.Payload,
) ([]metav1.Condition, error) {
	var spec api.PostgresUserSpec
	if err := json.Unmarshal(payload.Spec, &spec); err != nil {
		return nil, fmt.Errorf("decode user directive: %w", err)
	}
	if payload.Deleting {
		return e.runSQLJob(ctx, desired, payload,
			"DROP ROLE IF EXISTS "+quoteIdentifier(spec.RoleName)+";\n", nil)
	}
	if e.Secrets == nil {
		return nil, errors.New("user directive requires Vault secret resolution")
	}
	credential, err := e.Secrets.Resolve(ctx, desired, spec.PasswordVaultRef)
	if err != nil {
		return nil, err
	}
	if err := vault.RequireFields(credential, spec.PasswordVaultRef.Key); err != nil {
		return nil, fmt.Errorf("user credential schema: %w", err)
	}
	sql := userSQL(spec)
	conditions, err := e.runSQLJob(ctx, desired, payload, sql,
		[]byte(credential.Data[spec.PasswordVaultRef.Key]))
	if err != nil {
		return conditions, err
	}
	conditions = append(conditions, metav1.Condition{
		Type: "CredentialVersion", Status: metav1.ConditionTrue,
		Reason: "VaultVersionApplied", Message: strconv.FormatInt(credential.Version, 10),
		LastTransitionTime: metav1.Now(),
	})
	return conditions, nil
}

func (e *DirectiveExecutor) runSQLJob(ctx context.Context, desired plan.SitePlan,
	payload directive.Payload, sql string, userPassword []byte,
) ([]metav1.Condition, error) {
	name := operationName(payload.OperationUID)
	key := client.ObjectKey{Namespace: desired.Site.Namespace, Name: name}
	var observed batchv1.Job
	if err := e.Client.Get(ctx, key, &observed); err == nil {
		return waitForJob(ctx, e.Client, key, "SQLApplied",
			"PostgreSQL accepted the idempotent declaration")
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}
	if len(userPassword) > 0 {
		if err := e.Client.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: name + "-credential"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"password": userPassword},
		}); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
	}
	job := sqlJob(desired, payload, name, sql, len(userPassword) > 0)
	if err := e.Client.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	conditions, err := waitForJob(ctx, e.Client, key, "SQLApplied",
		"PostgreSQL accepted the idempotent declaration")
	if len(userPassword) > 0 {
		_ = e.Client.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Namespace: key.Namespace, Name: name + "-credential",
		}})
	}
	return conditions, err
}

func sqlJob(desired plan.SitePlan, payload directive.Payload, name, sql string, userPassword bool) *batchv1.Job {
	backoff := int32(3)
	deadline := int64(300)
	ttl := int32(86400)
	host := payload.Primary + "." + desired.Site.Namespace + ".svc"
	volumes := []corev1.Volume{{Name: "tls", VolumeSource: corev1.VolumeSource{
		Secret: &corev1.SecretVolumeSource{SecretName: payload.Primary + "-tls"},
	}}}
	mounts := []corev1.VolumeMount{{Name: "tls", MountPath: "/tls", ReadOnly: true}}
	if userPassword {
		volumes = append(volumes, corev1.Volume{Name: "user-credential", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: name + "-credential"},
		}})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "user-credential", MountPath: "/user-credential", ReadOnly: true,
		})
	}
	preflight := `SELECT NOT pg_is_in_recovery() AS is_primary \gset
\if :is_primary
\else
\quit 1
\endif
`
	command := "psql -X -v ON_ERROR_STOP=1 -f - <<'SQL'\n" + preflight + sql + "SQL\n"
	if userPassword {
		command = "export USER_PASSWORD=\"$(cat /user-credential/password)\"\n" + command
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: desired.Site.Namespace, Name: name,
			Labels: map[string]string{
				"multisite-postgres.dev/instance-uid":  payload.InstanceUID,
				"multisite-postgres.dev/operation-uid": payload.OperationUID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline, TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					"multisite-postgres.dev/operation-uid": payload.OperationUID,
				}},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					ServiceAccountName:            workloadServiceAccount,
					AutomountServiceAccountToken:  ptr(false),
					TerminationGracePeriodSeconds: ptr(int64(30)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr(true), SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name: "psql", Image: desired.Postgres.Image,
						Command: []string{"/bin/bash", "-ec", command},
						Env: []corev1.EnvVar{
							{Name: "PGHOST", Value: host},
							{Name: "PGPORT", Value: "5432"},
							{Name: "PGDATABASE", Value: "postgres"},
							{Name: "PGUSER", ValueFrom: secretKeySelector(
								"postgres-auth", "superuser-username")},
							{Name: "PGSSLMODE", Value: "verify-full"},
							{Name: "PGSSLROOTCERT", Value: "/tls/ca.crt"},
							{Name: "PGPASSWORD", ValueFrom: secretKeySelector(
								"postgres-auth", "superuser-password")},
						},
						SecurityContext: restrictedContainer(),
						VolumeMounts:    mounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

func waitForJob(ctx context.Context, kube client.Client,
	key client.ObjectKey, reason, message string,
) ([]metav1.Condition, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		var job batchv1.Job
		if err := kube.Get(ctx, key, &job); err != nil {
			return nil, err
		}
		for _, condition := range job.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			switch condition.Type {
			case batchv1.JobComplete:
				return []metav1.Condition{{
					Type: "Succeeded", Status: metav1.ConditionTrue,
					Reason: reason, Message: message,
					LastTransitionTime: metav1.Now(),
				}}, nil
			case batchv1.JobFailed:
				return nil, fmt.Errorf("SQL Job failed: %s", condition.Message)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func databaseSQL(spec api.PostgresDatabaseSpec, tdeEnabled, deleting bool) (string, error) {
	database := quoteIdentifier(spec.DatabaseName)
	if deleting {
		return "DROP DATABASE IF EXISTS " + database + " WITH (FORCE);\n", nil
	}
	owner := ""
	for _, role := range spec.Roles {
		if role.Profile == "Owner" {
			if owner != "" {
				return "", errors.New("database has multiple Owner roles")
			}
			owner = role.Name
		}
	}
	var sql strings.Builder
	fmt.Fprintf(&sql, "SELECT pg_advisory_lock(hashtextextended(%s, 0));\n",
		quoteLiteral("mspsql/database/"+spec.DatabaseName))
	fmt.Fprintf(&sql, "SELECT format('CREATE DATABASE %%I TEMPLATE template1', %s) "+
		"WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = %s) \\gexec\n",
		quoteLiteral(spec.DatabaseName), quoteLiteral(spec.DatabaseName))
	for _, role := range spec.Roles {
		fmt.Fprintf(&sql, "DO $$BEGIN CREATE ROLE %s NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END$$;\n",
			quoteIdentifier(role.Name))
	}
	if owner != "" {
		fmt.Fprintf(&sql, "ALTER DATABASE %s OWNER TO %s;\n", database, quoteIdentifier(owner))
	}
	if spec.Quotas.ConnectionLimit != nil {
		fmt.Fprintf(&sql, "ALTER DATABASE %s CONNECTION LIMIT %d;\n",
			database, *spec.Quotas.ConnectionLimit)
	}
	appendDatabaseSetting(&sql, database, "statement_timeout", spec.Quotas.StatementTimeout)
	appendDatabaseSetting(&sql, database, "lock_timeout", spec.Quotas.LockTimeout)
	appendDatabaseSetting(&sql, database, "idle_in_transaction_session_timeout",
		spec.Quotas.IdleInTransactionSessionTimeout)
	if spec.Quotas.TempFileLimit != nil {
		kilobytes := (spec.Quotas.TempFileLimit.Value() + 1023) / 1024
		fmt.Fprintf(&sql, "ALTER DATABASE %s SET temp_file_limit = %s;\n",
			database, quoteLiteral(fmt.Sprintf("%dkB", kilobytes)))
	}
	fmt.Fprintf(&sql, "\\connect %s\n", database)
	schemas := slices.Clone(spec.Schemas)
	slices.Sort(schemas)
	for _, schema := range schemas {
		authorization := "postgres"
		if owner != "" {
			authorization = quoteIdentifier(owner)
		}
		fmt.Fprintf(&sql, "CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s;\n",
			quoteIdentifier(schema), authorization)
	}
	for _, role := range spec.Roles {
		appendDatabaseRoleSQL(&sql, spec.DatabaseName, schemas, owner, role)
	}
	if tdeEnabled {
		sql.WriteString(`SELECT count(*) = 0 AS tde_verified
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_am a ON a.oid = c.relam
WHERE c.relkind IN ('r','m')
  AND n.nspname NOT IN ('pg_catalog','information_schema')
  AND n.nspname !~ '^pg_toast'
  AND a.amname <> 'tde_heap' \gset
\if :tde_verified
\else
\quit 1
\endif
`)
	}
	return sql.String(), nil
}

func appendDatabaseSetting(sql *strings.Builder, database, setting string,
	duration *metav1.Duration,
) {
	if duration == nil {
		return
	}
	fmt.Fprintf(sql, "ALTER DATABASE %s SET %s = %s;\n", database, setting,
		quoteLiteral(fmt.Sprintf("%dms", duration.Milliseconds())))
}

func appendDatabaseRoleSQL(sql *strings.Builder, database string, schemas []string, owner string,
	role api.DatabaseRole,
) {
	quotedRole := quoteIdentifier(role.Name)
	fmt.Fprintf(sql, "GRANT CONNECT ON DATABASE %s TO %s;\n", quoteIdentifier(database), quotedRole)
	for _, schema := range schemas {
		quotedSchema := quoteIdentifier(schema)
		switch role.Profile {
		case "Owner":
			fmt.Fprintf(sql, "GRANT ALL ON SCHEMA %s TO %s;\n", quotedSchema, quotedRole)
		case "ReadWrite":
			fmt.Fprintf(sql, "GRANT USAGE, CREATE ON SCHEMA %s TO %s;\n", quotedSchema, quotedRole)
			fmt.Fprintf(sql, "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %s TO %s;\n",
				quotedSchema, quotedRole)
			fmt.Fprintf(sql, "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA %s TO %s;\n",
				quotedSchema, quotedRole)
		case "ReadOnly":
			fmt.Fprintf(sql, "GRANT USAGE ON SCHEMA %s TO %s;\n", quotedSchema, quotedRole)
			fmt.Fprintf(sql, "GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;\n", quotedSchema, quotedRole)
			fmt.Fprintf(sql, "GRANT SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s;\n", quotedSchema, quotedRole)
		}
		if owner != "" && role.Profile != "Owner" {
			privileges := "SELECT"
			if role.Profile == "ReadWrite" {
				privileges = "SELECT, INSERT, UPDATE, DELETE"
			}
			fmt.Fprintf(sql, "ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s "+
				"GRANT %s ON TABLES TO %s;\n", quoteIdentifier(owner), quotedSchema, privileges, quotedRole)
		}
	}
}

func userSQL(spec api.PostgresUserSpec) string {
	role := quoteIdentifier(spec.RoleName)
	var sql strings.Builder
	fmt.Fprintf(&sql, "SELECT pg_advisory_lock(hashtextextended(%s, 0));\n",
		quoteLiteral("mspsql/user/"+spec.RoleName))
	fmt.Fprintf(&sql, "DO $$BEGIN CREATE ROLE %s LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END$$;\n", role)
	sql.WriteString("\\getenv user_password USER_PASSWORD\n")
	fmt.Fprintf(&sql, "ALTER ROLE %s LOGIN PASSWORD :'user_password';\n", role)
	for _, membership := range spec.MemberOf {
		fmt.Fprintf(&sql, "GRANT %s TO %s;\n", quoteIdentifier(membership.Role), role)
	}
	if spec.Quotas.ConnectionLimit != nil {
		fmt.Fprintf(&sql, "ALTER ROLE %s CONNECTION LIMIT %d;\n", role, *spec.Quotas.ConnectionLimit)
	}
	appendRoleSetting(&sql, role, "statement_timeout", spec.Quotas.StatementTimeout)
	appendRoleSetting(&sql, role, "lock_timeout", spec.Quotas.LockTimeout)
	appendRoleSetting(&sql, role, "idle_in_transaction_session_timeout",
		spec.Quotas.IdleInTransactionSessionTimeout)
	if spec.Quotas.TempFileLimit != nil {
		kilobytes := (spec.Quotas.TempFileLimit.Value() + 1023) / 1024
		fmt.Fprintf(&sql, "ALTER ROLE %s SET temp_file_limit = %s;\n", role,
			quoteLiteral(fmt.Sprintf("%dkB", kilobytes)))
	}
	fmt.Fprintf(&sql, "SELECT pg_advisory_unlock(hashtextextended(%s, 0));\n",
		quoteLiteral("mspsql/user/"+spec.RoleName))
	return sql.String()
}

func appendRoleSetting(sql *strings.Builder, role, setting string, duration *metav1.Duration) {
	if duration == nil {
		return
	}
	fmt.Fprintf(sql, "ALTER ROLE %s SET %s = %s;\n", role, setting,
		quoteLiteral(fmt.Sprintf("%dms", duration.Milliseconds())))
}

func operationName(operationUID string) string {
	return "mspsql-sql-" + operationHash(operationUID)
}

func operationHash(operationUID string) string {
	sum := sha256.Sum256([]byte(operationUID))
	return hex.EncodeToString(sum[:8])
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
