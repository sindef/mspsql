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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sindef/mspsql/internal/plan"
)

type Payload struct {
	ProtocolVersion string          `json:"protocolVersion"`
	SiteUID         string          `json:"siteUID"`
	InstanceUID     string          `json:"instanceUID"`
	ObjectUID       string          `json:"objectUID"`
	OperationUID    string          `json:"operationUID"`
	Type            string          `json:"type"`
	Primary         string          `json:"primary,omitempty"`
	BackupSource    string          `json:"backupSource,omitempty"`
	BackupType      string          `json:"backupType,omitempty"`
	Deleting        bool            `json:"deleting,omitempty"`
	GeneratedAt     time.Time       `json:"generatedAt"`
	Spec            json.RawMessage `json:"spec"`
}

func Sign(privateKey ed25519.PrivateKey, payload Payload) (plan.Envelope, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return plan.Envelope{}, errors.New("invalid Ed25519 private key")
	}
	if payload.SiteUID == "" || payload.InstanceUID == "" || payload.ObjectUID == "" ||
		payload.OperationUID == "" || payload.Type == "" {
		return plan.Envelope{}, errors.New("directive identity is incomplete")
	}
	payload.ProtocolVersion = plan.ProtocolVersion
	encoded, err := plan.Canonical(payload)
	if err != nil {
		return plan.Envelope{}, err
	}
	return plan.Envelope{
		Plan:      encoded,
		Signature: base64.RawStdEncoding.EncodeToString(ed25519.Sign(privateKey, encoded)),
	}, nil
}

func Verify(publicKey ed25519.PublicKey, envelope plan.Envelope, siteUID, instanceUID,
	operationUID string,
) (Payload, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return Payload{}, errors.New("invalid Ed25519 public key")
	}
	signature, err := base64.RawStdEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return Payload{}, fmt.Errorf("decode signature: %w", err)
	}
	if !ed25519.Verify(publicKey, envelope.Plan, signature) {
		return Payload{}, errors.New("directive signature is invalid")
	}
	var payload Payload
	if err := json.Unmarshal(envelope.Plan, &payload); err != nil {
		return Payload{}, fmt.Errorf("decode directive: %w", err)
	}
	if payload.ProtocolVersion != plan.ProtocolVersion {
		return Payload{}, fmt.Errorf("unsupported protocol version %q", payload.ProtocolVersion)
	}
	if payload.SiteUID != siteUID || payload.InstanceUID != instanceUID ||
		payload.OperationUID != operationUID {
		return Payload{}, errors.New("directive identity does not match this site and operation")
	}
	return payload, nil
}
