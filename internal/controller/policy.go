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
	"fmt"
	"slices"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func validateSitePolicy(site api.PostgresSiteSpec, registration *api.SiteRegistration) error {
	if registration.Spec.Revoked {
		return fmt.Errorf("site registration %q is revoked", registration.Name)
	}
	if site.Storage.Etcd != nil &&
		!contains(registration.Spec.PermittedStorageClasses.Etcd, site.Storage.Etcd.StorageClassName) {
		return fmt.Errorf("etcd StorageClass %q is not permitted", site.Storage.Etcd.StorageClassName)
	}
	if site.Storage.Postgres != nil &&
		!contains(registration.Spec.PermittedStorageClasses.Postgres, site.Storage.Postgres.StorageClassName) {
		return fmt.Errorf("PostgreSQL StorageClass %q is not permitted", site.Storage.Postgres.StorageClassName)
	}
	if site.LoadBalancer != nil &&
		!contains(registration.Spec.MetallbAddressPools, site.LoadBalancer.AddressPool) {
		return fmt.Errorf("MetalLB address pool %q is not permitted", site.LoadBalancer.AddressPool)
	}
	if !issuerAllowed(registration.Spec.PermittedIssuers.Etcd, site.Certificates.EtcdIssuerRef) {
		return fmt.Errorf("etcd issuer %q is not permitted", site.Certificates.EtcdIssuerRef.Name)
	}
	if site.Role == api.SiteRoleData {
		if !issuerAllowed(registration.Spec.PermittedIssuers.Postgres, site.Certificates.PostgresIssuerRef) {
			return fmt.Errorf("PostgreSQL issuer %q is not permitted", site.Certificates.PostgresIssuerRef.Name)
		}
		if !issuerAllowed(registration.Spec.PermittedIssuers.Pgpool, site.Certificates.PgpoolIssuerRef) {
			return fmt.Errorf("pgpool issuer %q is not permitted", site.Certificates.PgpoolIssuerRef.Name)
		}
		if site.Certificates.BackupIssuerRef.Name != "" {
			permittedBackupIssuers := registration.Spec.PermittedIssuers.Backup
			if len(permittedBackupIssuers) == 0 {
				permittedBackupIssuers = registration.Spec.PermittedIssuers.Postgres
			}
			if !issuerAllowed(permittedBackupIssuers, site.Certificates.BackupIssuerRef) {
				return fmt.Errorf("backup issuer %q is not permitted", site.Certificates.BackupIssuerRef.Name)
			}
		}
	}
	if !discoveredStorageClass(registration.Status.DiscoveredStorageClasses, site.Storage.Etcd) ||
		!discoveredStorageClass(registration.Status.DiscoveredStorageClasses, site.Storage.Postgres) {
		return fmt.Errorf("requested StorageClass is not present in the site inventory")
	}
	return nil
}

func discoveredStorageClass(inventory []api.StorageClassInventory, requested *api.StorageRequest) bool {
	if requested == nil {
		return true
	}
	for _, item := range inventory {
		if item.Name == requested.StorageClassName {
			return true
		}
	}
	return false
}

func issuerAllowed(allowed []api.IssuerReference, requested api.IssuerReference) bool {
	for _, item := range allowed {
		if item.Name == requested.Name && item.Kind == requested.Kind && item.Group == requested.Group {
			return true
		}
	}
	return false
}

func contains(values []string, wanted string) bool {
	return slices.Contains(values, wanted)
}
