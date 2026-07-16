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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/directive"
	"github.com/sindef/mspsql/internal/plan"
)

func TestDatabaseSQLIsIdempotentAndAuditsTDE(t *testing.T) {
	sql, err := databaseSQL(api.PostgresDatabaseSpec{
		DatabaseName: "orders-api",
		Schemas:      []string{"app"},
		Roles: []api.DatabaseRole{
			{Name: "orders_owner", Profile: "Owner"},
			{Name: "orders_rw", Profile: "ReadWrite"},
			{Name: "orders_ro", Profile: "ReadOnly"},
		},
	}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"WHERE NOT EXISTS (SELECT FROM pg_database", `CREATE SCHEMA IF NOT EXISTS "app"`,
		"ALTER DEFAULT PRIVILEGES", "a.amname <> 'tde_heap'", `ALTER DATABASE "orders-api" OWNER`,
	} {
		if !strings.Contains(sql, expected) {
			t.Fatalf("database SQL is missing %q:\n%s", expected, sql)
		}
	}
	createOwner := strings.Index(sql, `CREATE ROLE "orders_owner" NOLOGIN`)
	changeOwner := strings.Index(sql, `ALTER DATABASE "orders-api" OWNER TO "orders_owner"`)
	if createOwner == -1 || changeOwner == -1 || createOwner > changeOwner {
		t.Fatalf("owner role must be created before database ownership changes:\n%s", sql)
	}
}

func TestUserSQLReadsPasswordFromEnvironment(t *testing.T) {
	timeout := metav1.Duration{Duration: 15 * time.Second}
	sql := userSQL(api.PostgresUserSpec{
		RoleName: "orders_app",
		MemberOf: []api.RoleMembership{{Role: "orders_rw"}},
		Quotas: api.RoleQuotas{
			ConnectionLimit:  ptr(int32(50)),
			StatementTimeout: &timeout,
		},
	})
	if !strings.Contains(sql, `\getenv user_password USER_PASSWORD`) ||
		!strings.Contains(sql, `GRANT "orders_rw" TO "orders_app"`) ||
		!strings.Contains(sql, `pg_advisory_lock`) ||
		!strings.Contains(sql, `pg_advisory_unlock`) {
		t.Fatalf("user SQL = %s", sql)
	}
	job := sqlJob(plan.SitePlan{
		InstanceUID: "instance",
		Site:        api.PostgresSiteSpec{Namespace: "orders"},
		Postgres:    api.PostgresSpec{Image: "postgres:17"},
	}, directive.Payload{
		InstanceUID: "instance", OperationUID: "operation", Primary: "postgres-vic-0",
	}, "operation", sql, true)
	command := job.Spec.Template.Spec.Containers[0].Command[2]
	if !strings.Contains(command, "cat /user-credential/password") ||
		!strings.Contains(command, "pg_is_in_recovery()") {
		t.Fatalf("Job command does not load the mounted password: %s", command)
	}
}

func TestBackupCoordinatorUsesSynchronousStandby(t *testing.T) {
	desired := plan.SitePlan{
		InstanceUID: "instance",
		Backup: &api.BackupSpec{
			Repository: api.BackupRepositorySpec{
				Bucket: "backups", Prefix: "orders", Region: "ap-southeast-2",
				Endpoint: "https://minio.example:9443", URIStyle: "path",
			},
			Retention: api.BackupRetention{Duration: metav1.Duration{Duration: 8 * 24 * time.Hour}},
		},
		MemberAddresses: map[string]string{
			"postgres-vic-0": "10.0.0.1", "postgres-qld-0": "10.0.1.1",
		},
	}
	config, err := backupCoordinatorConfig(desired, directive.Payload{
		Primary: "postgres-vic-0", BackupSource: "postgres-qld-0", BackupType: "full",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"backup-standby=y", "pg1-host=10.0.0.1", "pg2-host=10.0.1.1",
		"repo1-retention-full=8", "repo1-storage-port=9443",
	} {
		if !strings.Contains(config, expected) {
			t.Fatalf("backup config is missing %q:\n%s", expected, config)
		}
	}
}
