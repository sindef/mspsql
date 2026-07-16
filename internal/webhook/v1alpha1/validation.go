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

package v1alpha1

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/robfig/cron/v3"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

var protectedRoles = map[string]struct{}{
	"postgres": {}, "replication": {}, "patroni": {}, "pgbackrest": {}, "pgpool": {},
}

func defaultIssuer(ref *api.IssuerReference) {
	if ref.Kind == "" {
		ref.Kind = "ClusterIssuer"
	}
	if ref.Group == "" {
		ref.Group = "cert-manager.io"
	}
}

func defaultInstance(obj *api.MultiSitePostgres) {
	if obj.Spec.DeletionPolicy == "" {
		obj.Spec.DeletionPolicy = api.DeletionPolicyRetain
	}
	if obj.Spec.Backup != nil {
		if obj.Spec.Backup.Retention.Duration.Duration == 0 {
			obj.Spec.Backup.Retention.Duration.Duration = 7 * 24 * time.Hour
		}
		if obj.Spec.Backup.Retention.WALDuration.Duration == 0 {
			obj.Spec.Backup.Retention.WALDuration.Duration = 7 * 24 * time.Hour
		}
		if obj.Spec.Backup.Repository.Region == "" {
			obj.Spec.Backup.Repository.Region = "us-east-1"
		}
		if obj.Spec.Backup.Repository.URIStyle == "" {
			obj.Spec.Backup.Repository.URIStyle = "host"
		}
	}
	for i := range obj.Spec.Sites {
		defaultIssuer(&obj.Spec.Sites[i].Certificates.EtcdIssuerRef)
		defaultIssuer(&obj.Spec.Sites[i].Certificates.PostgresIssuerRef)
		defaultIssuer(&obj.Spec.Sites[i].Certificates.PgpoolIssuerRef)
	}
}

func validateInstance(obj *api.MultiSitePostgres) error {
	var errs field.ErrorList
	specPath := field.NewPath("spec")
	var etcd, postgres int32
	siteNames := map[string]struct{}{}
	siteNamespaces := map[string]struct{}{}

	for i, site := range obj.Spec.Sites {
		p := specPath.Child("sites").Index(i)
		if _, found := siteNames[site.Name]; found {
			errs = append(errs, field.Duplicate(p.Child("name"), site.Name))
		}
		siteNames[site.Name] = struct{}{}
		namespaceKey := site.SiteRegistrationRef + "/" + site.Namespace
		if _, found := siteNamespaces[namespaceKey]; found {
			errs = append(errs, field.Duplicate(p.Child("namespace"), namespaceKey))
		}
		siteNamespaces[namespaceKey] = struct{}{}
		for _, msg := range validation.IsDNS1123Label(site.Namespace) {
			errs = append(errs, field.Invalid(p.Child("namespace"), site.Namespace, msg))
		}
		etcd += site.Components.EtcdReplicas
		postgres += site.Components.PostgresReplicas
		errs = append(errs, validateDataSite(site, p)...)
	}
	if etcd < 3 || etcd%2 == 0 {
		errs = append(errs, field.Invalid(specPath.Child("sites"), etcd,
			"total etcd voters must be odd and at least three"))
	}
	if postgres < 2 {
		errs = append(errs, field.Invalid(specPath.Child("sites"), postgres,
			"at least two PostgreSQL replicas are required"))
	}
	if obj.Spec.Postgres.SynchronousStandbyCount >= postgres {
		errs = append(errs, field.Invalid(specPath.Child("postgres", "synchronousStandbyCount"),
			obj.Spec.Postgres.SynchronousStandbyCount, "must be lower than total PostgreSQL replicas"))
	}
	if obj.Spec.TDE.Enabled && obj.Spec.TDE.Vault == nil {
		errs = append(errs, field.Required(specPath.Child("tde", "vault"), "TDE requires a Vault key identity"))
	}
	if obj.Spec.Credentials.PostgresVaultRef.Mount == "" ||
		obj.Spec.Credentials.PostgresVaultRef.Path == "" {
		errs = append(errs, field.Required(specPath.Child("credentials", "postgresVaultRef"),
			"mount and path are required"))
	}
	hasPgpool := false
	for _, site := range obj.Spec.Sites {
		hasPgpool = hasPgpool || site.Components.PgpoolReplicas > 0
	}
	if hasPgpool && (obj.Spec.Credentials.PgpoolVaultRef.Mount == "" ||
		obj.Spec.Credentials.PgpoolVaultRef.Path == "") {
		errs = append(errs, field.Required(specPath.Child("credentials", "pgpoolVaultRef"),
			"mount and path are required when Pgpool is enabled"))
	}
	if obj.Spec.Backup != nil && strings.Trim(obj.Spec.Backup.Repository.Prefix, "/") == "" {
		errs = append(errs, field.Required(specPath.Child("backup", "repository", "prefix"),
			"backup prefix must identify this instance"))
	}
	if obj.Spec.Backup != nil {
		errs = append(errs, validateBackupRepository(obj.Spec.Backup.Repository,
			specPath.Child("backup", "repository"))...)
		errs = append(errs, validateBackupSchedules(obj.Spec.Backup.Schedules,
			specPath.Child("backup", "schedules"))...)
	}
	return errs.ToAggregate()
}

func validateBackupRepository(repository api.BackupRepositorySpec, repositoryPath *field.Path) field.ErrorList {
	var errs field.ErrorList
	for name, value := range map[string]string{
		"bucket": repository.Bucket, "prefix": repository.Prefix, "region": repository.Region,
	} {
		if strings.ContainsAny(value, "\r\n") {
			errs = append(errs, field.Invalid(repositoryPath.Child(name), value, "must be a single line"))
		}
	}
	if repository.Endpoint != "" {
		endpoint, err := url.Parse(repository.Endpoint)
		if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" {
			errs = append(errs, field.Invalid(repositoryPath.Child("endpoint"),
				repository.Endpoint, "must be an absolute HTTPS URL"))
		}
	}
	if ref := repository.CABundleSecretRef; ref != nil && ref.Name == "" {
		errs = append(errs, field.Required(repositoryPath.Child("caBundleSecretRef", "name"), "required"))
	}
	return errs
}

func validateBackupSchedules(schedules api.BackupSchedules, schedulesPath *field.Path) field.ErrorList {
	timezone := schedules.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return field.ErrorList{field.Invalid(schedulesPath.Child("timezone"), schedules.Timezone,
			"must be an IANA timezone")}
	}
	var errs field.ErrorList
	for name, expression := range map[string]string{
		"full": schedules.Full, "differential": schedules.Differential, "incremental": schedules.Incremental,
	} {
		if expression == "" {
			continue
		}
		if _, err := cron.ParseStandard("CRON_TZ=" + timezone + " " + expression); err != nil {
			errs = append(errs, field.Invalid(schedulesPath.Child(name), expression, err.Error()))
		}
	}
	return errs
}

func validateDataSite(site api.PostgresSiteSpec, sitePath *field.Path) field.ErrorList {
	if site.Role == api.SiteRoleWitness {
		if site.Components.PostgresReplicas != 0 || site.Components.PgpoolReplicas != 0 {
			return field.ErrorList{field.Invalid(sitePath.Child("components"), site.Components,
				"witness sites cannot run PostgreSQL or Pgpool")}
		}
		return nil
	}
	var errs field.ErrorList
	if site.Components.PostgresReplicas == 0 {
		errs = append(errs, field.Invalid(sitePath.Child("components", "postgresReplicas"), 0,
			"data sites require at least one PostgreSQL replica"))
	}
	if site.Storage.Postgres == nil || site.Storage.Etcd == nil {
		errs = append(errs, field.Required(sitePath.Child("storage"),
			"data sites require etcd and PostgreSQL storage"))
	}
	if site.VaultAuth == nil {
		return append(errs, field.Required(sitePath.Child("vaultAuth"),
			"data sites require Vault authentication"))
	}
	vaultPath := sitePath.Child("vaultAuth")
	address, err := url.Parse(site.VaultAuth.Address)
	if err != nil || address.Scheme == "" || address.Host == "" {
		errs = append(errs, field.Invalid(vaultPath.Child("address"),
			site.VaultAuth.Address, "must be an absolute URL"))
	}
	if site.VaultAuth.AuthMount == "" {
		errs = append(errs, field.Required(vaultPath.Child("authMount"), "required"))
	}
	if site.VaultAuth.AuthRole == "" {
		errs = append(errs, field.Required(vaultPath.Child("authRole"), "required"))
	}
	if ref := site.VaultAuth.CABundleSecretRef; ref != nil && ref.Name == "" {
		errs = append(errs, field.Required(vaultPath.Child("caBundleSecretRef", "name"), "required"))
	}
	return errs
}

func validateDatabase(obj *api.PostgresDatabase) error {
	var errs field.ErrorList
	if obj.Spec.InstanceRef == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "instanceRef"), "instance is required"))
	}
	if msgs := validation.IsDNS1123Subdomain(obj.Spec.DatabaseName); len(msgs) > 0 {
		errs = append(errs, field.Invalid(field.NewPath("spec", "databaseName"), obj.Spec.DatabaseName,
			strings.Join(msgs, "; ")))
	}
	errs = append(errs, validateSQLIdentifier(field.NewPath("spec", "databaseName"),
		obj.Spec.DatabaseName)...)
	seen := map[string]struct{}{}
	owners := 0
	for i, role := range obj.Spec.Roles {
		p := field.NewPath("spec", "roles").Index(i).Child("name")
		errs = append(errs, validateSQLIdentifier(p, role.Name)...)
		if _, protected := protectedRoles[role.Name]; protected {
			errs = append(errs, field.Forbidden(p, "infrastructure role is protected"))
		}
		if _, found := seen[role.Name]; found {
			errs = append(errs, field.Duplicate(p, role.Name))
		}
		seen[role.Name] = struct{}{}
		if role.Profile == "Owner" {
			owners++
		}
	}
	if owners > 1 {
		errs = append(errs, field.Invalid(field.NewPath("spec", "roles"), owners,
			"at most one Owner role is permitted"))
	}
	for i, schema := range obj.Spec.Schemas {
		errs = append(errs, validateSQLIdentifier(
			field.NewPath("spec", "schemas").Index(i), schema)...)
	}
	return errs.ToAggregate()
}

func validateUser(obj *api.PostgresUser) error {
	var errs field.ErrorList
	errs = append(errs, validateSQLIdentifier(field.NewPath("spec", "roleName"), obj.Spec.RoleName)...)
	if _, protected := protectedRoles[obj.Spec.RoleName]; protected {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "roleName"),
			"infrastructure role is protected"))
	}
	if obj.Spec.PasswordVaultRef.Mount == "" || obj.Spec.PasswordVaultRef.Path == "" ||
		obj.Spec.PasswordVaultRef.Key == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "passwordVaultRef"),
			"mount, path and key are required"))
	}
	for i, membership := range obj.Spec.MemberOf {
		errs = append(errs, validateSQLIdentifier(
			field.NewPath("spec", "memberOf").Index(i).Child("role"), membership.Role)...)
	}
	return errs.ToAggregate()
}

func validateSQLIdentifier(identifierPath *field.Path, value string) field.ErrorList {
	if value == "" {
		return field.ErrorList{field.Required(identifierPath, "required")}
	}
	if len(value) > 63 {
		return field.ErrorList{field.TooLong(identifierPath, value, 63)}
	}
	if !utf8.ValidString(value) {
		return field.ErrorList{field.Invalid(identifierPath, value, "must be valid UTF-8")}
	}
	for _, character := range value {
		if character == 0 || unicode.IsControl(character) {
			return field.ErrorList{field.Invalid(identifierPath, value,
				"must not contain NUL or control characters")}
		}
	}
	return nil
}

func validateRestore(obj *api.PostgresRestore) error {
	if obj.Spec.SourceInstanceRef == obj.Spec.TargetInstanceRef {
		return field.Invalid(field.NewPath("spec", "targetInstanceRef"), obj.Spec.TargetInstanceRef,
			"in-place restore is forbidden")
	}
	if obj.Spec.TargetTime.IsZero() {
		return field.Required(field.NewPath("spec", "targetTime"), "time-based PITR target is required")
	}
	return nil
}

func validateUpgrade(obj *api.PostgresUpgrade) error {
	if obj.Spec.InstanceRef == "" || obj.Spec.TargetImage == "" {
		return fmt.Errorf("instanceRef and targetImage are required")
	}
	if obj.Spec.ServiceRestorationTarget.Duration <= 0 {
		return field.Invalid(field.NewPath("spec", "serviceRestorationTarget"),
			obj.Spec.ServiceRestorationTarget, "must be positive")
	}
	return nil
}
