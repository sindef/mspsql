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
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestValidateInstance(t *testing.T) {
	storage := func() *api.StorageRequest {
		return &api.StorageRequest{StorageClassName: "standard", Size: resource.MustParse("1Gi")}
	}
	base := &api.MultiSitePostgres{
		Spec: api.MultiSitePostgresSpec{
			Postgres: api.PostgresSpec{
				MajorVersion:            17,
				Image:                   "postgres:17",
				SynchronousStandbyCount: 1,
			},
			Sites: []api.PostgresSiteSpec{
				{
					Name: "a", SiteRegistrationRef: "a", Namespace: "postgres",
					Role: api.SiteRoleData, Components: api.SiteComponents{
						EtcdReplicas: 1, PostgresReplicas: 1, PgpoolReplicas: 1,
					},
					Storage:   api.SiteStorage{Etcd: storage(), Postgres: storage()},
					VaultAuth: &api.VaultAuthSpec{Address: "https://vault", AuthMount: "kubernetes", AuthRole: "a"},
				},
				{
					Name: "b", SiteRegistrationRef: "b", Namespace: "postgres",
					Role: api.SiteRoleData, Components: api.SiteComponents{
						EtcdReplicas: 1, PostgresReplicas: 1, PgpoolReplicas: 1,
					},
					Storage:   api.SiteStorage{Etcd: storage(), Postgres: storage()},
					VaultAuth: &api.VaultAuthSpec{Address: "https://vault", AuthMount: "kubernetes", AuthRole: "b"},
				},
				{
					Name: "w", SiteRegistrationRef: "w", Namespace: "postgres",
					Role: api.SiteRoleWitness, Components: api.SiteComponents{EtcdReplicas: 1},
				},
			},
		},
	}

	if err := validateInstance(base); err != nil {
		t.Fatalf("valid topology rejected: %v", err)
	}

	base.Spec.Sites[2].Components.PostgresReplicas = 1
	if err := validateInstance(base); err == nil {
		t.Fatal("witness with PostgreSQL was accepted")
	}
}

func TestDefaultsAreNonDestructive(t *testing.T) {
	obj := &api.MultiSitePostgres{
		Spec: api.MultiSitePostgresSpec{
			Backup: &api.BackupSpec{},
			Sites:  []api.PostgresSiteSpec{{}},
		},
	}
	defaultInstance(obj)

	if obj.Spec.DeletionPolicy != api.DeletionPolicyRetain {
		t.Fatalf("deletion policy = %q", obj.Spec.DeletionPolicy)
	}
	if got := obj.Spec.Backup.Retention.Duration.Duration.Hours(); got != 168 {
		t.Fatalf("retention hours = %v", got)
	}
	if obj.Spec.Sites[0].Certificates.EtcdIssuerRef.Kind != "ClusterIssuer" {
		t.Fatal("issuer kind was not defaulted")
	}
}
