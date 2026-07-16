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

package plan

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

func TestSignVerify(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	desired := SitePlan{
		SiteUID: "site-1", InstanceUID: "instance-1", Revision: 4,
		Postgres: api.PostgresSpec{Parameters: map[string]string{"z": "1", "a": "2"}},
	}
	envelope, err := Sign(privateKey, desired)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify(publicKey, envelope, "site-1", "instance-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 4 {
		t.Fatalf("revision = %d", got.Revision)
	}
	if _, err := Verify(publicKey, envelope, "site-2", "instance-1", 3); err == nil {
		t.Fatal("plan for another site was accepted")
	}
	if _, err := Verify(publicKey, envelope, "site-1", "instance-1", 5); err == nil {
		t.Fatal("old plan revision was accepted")
	}
}

func TestClassify(t *testing.T) {
	previous := SitePlan{Revision: 1, Postgres: api.PostgresSpec{Image: "postgres:17"}}
	next := previous
	next.Revision = 2
	next.Postgres.Parameters = map[string]string{"log_min_duration_statement": "1000"}
	if got := Classify(previous, next); got != MutationSafe {
		t.Fatalf("parameter drift classified as %s", got)
	}
	next.Postgres.Image = "postgres:17.1"
	if got := Classify(previous, next); got != MutationCoordinated {
		t.Fatalf("image rollout classified as %s", got)
	}
}
