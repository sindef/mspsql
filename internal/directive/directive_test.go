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

package directive

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
)

func TestSignedDirectiveBindsSiteAndOperation(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := Sign(privateKey, Payload{
		SiteUID: "site", InstanceUID: "instance", OperationUID: "operation",
		Type: "Database", Spec: json.RawMessage(`{"databaseName":"orders"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := Verify(publicKey, envelope, "site", "instance", "operation")
	if err != nil {
		t.Fatal(err)
	}
	if payload.Type != "Database" {
		t.Fatalf("payload = %#v", payload)
	}
	if _, err := Verify(publicKey, envelope, "other-site", "instance", "operation"); err == nil {
		t.Fatal("directive was accepted for another site")
	}
}
