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
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

const ProtocolVersion = "v1alpha1"

type SitePlan struct {
	ProtocolVersion          string                      `json:"protocolVersion"`
	SiteUID                  string                      `json:"siteUID"`
	InstanceUID              string                      `json:"instanceUID"`
	HubNamespace             string                      `json:"hubNamespace"`
	HubName                  string                      `json:"hubName"`
	Revision                 int64                       `json:"revision"`
	GeneratedAt              time.Time                   `json:"generatedAt"`
	Site                     api.PostgresSiteSpec        `json:"site"`
	Postgres                 api.PostgresSpec            `json:"postgres"`
	TDE                      api.TDESpec                 `json:"tde,omitempty"`
	Backup                   *api.BackupSpec             `json:"backup,omitempty"`
	Credentials              api.InstanceCredentialsSpec `json:"credentials"`
	MemberAddresses          map[string]string           `json:"memberAddresses,omitempty"`
	AddressCandidates        map[string]string           `json:"addressCandidates,omitempty"`
	AddressMigration         *AddressMigrationPlan       `json:"addressMigration,omitempty"`
	CredentialRotation       *CredentialRotationPlan     `json:"credentialRotation,omitempty"`
	RuntimeCredentialVersion int64                       `json:"-"`
	Restore                  *RestorePlan                `json:"restore,omitempty"`
	Upgrade                  *UpgradePlan                `json:"upgrade,omitempty"`
	MajorUpgrade             *MajorUpgradePlan           `json:"majorUpgrade,omitempty"`
	Deletion                 *DeletionPlan               `json:"deletion,omitempty"`
}

type CredentialRotationPhase string

const (
	CredentialRotationPhaseCatalog  CredentialRotationPhase = "Catalog"
	CredentialRotationPhaseMember   CredentialRotationPhase = "Member"
	CredentialRotationPhaseRevoke   CredentialRotationPhase = "Revoke"
	CredentialRotationPhaseFinalize CredentialRotationPhase = "Finalize"
)

type CredentialRotationPlan struct {
	Version         int64                   `json:"version"`
	PreviousVersion int64                   `json:"previousVersion"`
	Phase           CredentialRotationPhase `json:"phase"`
	TargetMember    string                  `json:"targetMember,omitempty"`
	UpdatedMembers  []string                `json:"updatedMembers,omitempty"`
}

type AddressMigrationPlan struct {
	OperationUID string `json:"operationUID"`
	Member       string `json:"member"`
	OldAddress   string `json:"oldAddress"`
	NewAddress   string `json:"newAddress"`
}

type RestorePhase string

const (
	RestorePhaseSeed     RestorePhase = "Seed"
	RestorePhaseReplicas RestorePhase = "Replicas"
	RestorePhaseVerify   RestorePhase = "Verify"
)

type RestorePlan struct {
	OperationUID      string         `json:"operationUID"`
	SourceInstanceUID string         `json:"sourceInstanceUID"`
	SourceBackup      api.BackupSpec `json:"sourceBackup"`
	TargetTime        time.Time      `json:"targetTime"`
	BackupSet         string         `json:"backupSet,omitempty"`
	SeedSite          string         `json:"seedSite"`
	SeedMember        string         `json:"seedMember"`
	Phase             RestorePhase   `json:"phase"`
}

type UpgradePhase string

const (
	UpgradePhaseMember     UpgradePhase = "Member"
	UpgradePhaseSwitchover UpgradePhase = "Switchover"
	UpgradePhaseFinalize   UpgradePhase = "Finalize"
)

type UpgradePlan struct {
	OperationUID    string       `json:"operationUID"`
	TargetImage     string       `json:"targetImage"`
	TargetMember    string       `json:"targetMember,omitempty"`
	UpgradedMembers []string     `json:"upgradedMembers,omitempty"`
	FromPrimary     string       `json:"fromPrimary,omitempty"`
	Candidate       string       `json:"candidate,omitempty"`
	Phase           UpgradePhase `json:"phase"`
}

type MajorUpgradePhase string

const (
	MajorUpgradePhasePreflight             MajorUpgradePhase = "MajorPreflight"
	MajorUpgradePhaseDrain                 MajorUpgradePhase = "MajorDrain"
	MajorUpgradePhaseStop                  MajorUpgradePhase = "MajorStop"
	MajorUpgradePhaseSnapshot              MajorUpgradePhase = "MajorSnapshot"
	MajorUpgradePhaseUpgradePrimary        MajorUpgradePhase = "MajorUpgradePrimary"
	MajorUpgradePhaseStanzaUpgrade         MajorUpgradePhase = "MajorStanzaUpgrade"
	MajorUpgradePhaseStartPrimary          MajorUpgradePhase = "MajorStartPrimary"
	MajorUpgradePhaseRestoreWrites         MajorUpgradePhase = "MajorRestoreWrites"
	MajorUpgradePhaseReplicas              MajorUpgradePhase = "MajorReplicas"
	MajorUpgradePhaseFinalize              MajorUpgradePhase = "MajorFinalize"
	MajorUpgradePhaseRollback              MajorUpgradePhase = "MajorRollback"
	MajorUpgradePhaseRollbackStart         MajorUpgradePhase = "MajorRollbackStart"
	MajorUpgradePhaseRollbackRestoreWrites MajorUpgradePhase = "MajorRollbackRestoreWrites"
)

type MajorUpgradePlan struct {
	OperationUID      string                               `json:"operationUID"`
	Phase             MajorUpgradePhase                    `json:"phase"`
	Primary           string                               `json:"primary"`
	SourceMajor       int32                                `json:"sourceMajor"`
	TargetMajor       int32                                `json:"targetMajor"`
	TargetImage       string                               `json:"targetImage"`
	UpgradeImage      string                               `json:"upgradeImage"`
	RollbackRetention time.Duration                        `json:"rollbackRetention"`
	RollbackPolicies  map[string]api.StorageRollbackPolicy `json:"rollbackPolicies"`
}

type DeletionPlan struct {
	Policy api.DeletionPolicy `json:"policy"`
}

type Envelope struct {
	Plan      json.RawMessage `json:"plan"`
	Signature string          `json:"signature"`
}

func Canonical(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical JSON: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, data); err != nil {
		return nil, fmt.Errorf("compact canonical JSON: %w", err)
	}
	return compact.Bytes(), nil
}

func Sign(privateKey ed25519.PrivateKey, desired SitePlan) (Envelope, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return Envelope{}, errors.New("invalid Ed25519 private key")
	}
	if desired.Revision < 1 || desired.SiteUID == "" || desired.InstanceUID == "" {
		return Envelope{}, errors.New("plan identity and positive revision are required")
	}
	desired.ProtocolVersion = ProtocolVersion
	payload, err := Canonical(desired)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		Plan:      payload,
		Signature: base64.RawStdEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
	}, nil
}

func Verify(publicKey ed25519.PublicKey, envelope Envelope, expectedSiteUID, expectedInstanceUID string,
	minimumRevision int64,
) (SitePlan, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return SitePlan{}, errors.New("invalid Ed25519 public key")
	}
	signature, err := base64.RawStdEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return SitePlan{}, fmt.Errorf("decode signature: %w", err)
	}
	if !ed25519.Verify(publicKey, envelope.Plan, signature) {
		return SitePlan{}, errors.New("plan signature is invalid")
	}
	var desired SitePlan
	if err := json.Unmarshal(envelope.Plan, &desired); err != nil {
		return SitePlan{}, fmt.Errorf("decode plan: %w", err)
	}
	if desired.ProtocolVersion != ProtocolVersion {
		return SitePlan{}, fmt.Errorf("unsupported protocol version %q", desired.ProtocolVersion)
	}
	if desired.SiteUID != expectedSiteUID || desired.InstanceUID != expectedInstanceUID {
		return SitePlan{}, errors.New("plan identity does not match this site and instance")
	}
	if desired.Revision < minimumRevision {
		return SitePlan{}, fmt.Errorf("plan revision %d is older than %d", desired.Revision, minimumRevision)
	}
	return desired, nil
}

type MutationClass string

const (
	MutationSafe        MutationClass = "Safe"
	MutationCoordinated MutationClass = "Coordinated"
)

func Classify(previous, next SitePlan) MutationClass {
	if previous.Revision == 0 {
		return MutationCoordinated
	}
	if previous.Postgres.Image != next.Postgres.Image ||
		previous.Postgres.MajorVersion != next.Postgres.MajorVersion ||
		previous.Site.Components != next.Site.Components ||
		previous.Site.Certificates != next.Site.Certificates ||
		previous.Site.LoadBalancer == nil != (next.Site.LoadBalancer == nil) {
		return MutationCoordinated
	}
	if !bytes.Equal(mustCanonical(previous.Site.Storage), mustCanonical(next.Site.Storage)) {
		return MutationCoordinated
	}
	if previous.Site.LoadBalancer != nil &&
		previous.Site.LoadBalancer.AddressPool != next.Site.LoadBalancer.AddressPool {
		return MutationCoordinated
	}
	if !bytes.Equal(mustCanonical(previous.Backup), mustCanonical(next.Backup)) ||
		!bytes.Equal(mustCanonical(previous.TDE), mustCanonical(next.TDE)) ||
		!bytes.Equal(mustCanonical(previous.Credentials), mustCanonical(next.Credentials)) ||
		!bytes.Equal(mustCanonical(previous.AddressCandidates), mustCanonical(next.AddressCandidates)) ||
		!bytes.Equal(mustCanonical(previous.AddressMigration), mustCanonical(next.AddressMigration)) ||
		!bytes.Equal(mustCanonical(previous.CredentialRotation), mustCanonical(next.CredentialRotation)) ||
		!bytes.Equal(mustCanonical(previous.Restore), mustCanonical(next.Restore)) ||
		!bytes.Equal(mustCanonical(previous.Upgrade), mustCanonical(next.Upgrade)) ||
		!bytes.Equal(mustCanonical(previous.MajorUpgrade), mustCanonical(next.MajorUpgrade)) ||
		!bytes.Equal(mustCanonical(previous.Deletion), mustCanonical(next.Deletion)) {
		return MutationCoordinated
	}
	return MutationSafe
}

func mustCanonical(value any) []byte {
	data, err := Canonical(value)
	if err != nil {
		panic(err)
	}
	return data
}
