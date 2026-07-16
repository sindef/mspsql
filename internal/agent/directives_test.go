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
		!strings.Contains(sql, `GRANT "orders_rw" TO "orders_app"`) {
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
	if !strings.Contains(command, "cat /user-credential/password") {
		t.Fatalf("Job command does not load the mounted password: %s", command)
	}
}
