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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DeletionPolicy string

const (
	DeletionPolicyRetain DeletionPolicy = "Retain"
	DeletionPolicyDelete DeletionPolicy = "Delete"
)

type SiteRole string

const (
	SiteRoleData    SiteRole = "Data"
	SiteRoleWitness SiteRole = "Witness"
)

type IssuerReference struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	// +kubebuilder:default=ClusterIssuer
	Kind string `json:"kind,omitempty"`
	// +kubebuilder:default=cert-manager.io
	Group string `json:"group,omitempty"`
}

type StorageClassPolicy struct {
	Etcd             []string `json:"etcd,omitempty"`
	Postgres         []string `json:"postgres,omitempty"`
	RestoreWorkspace []string `json:"restoreWorkspace,omitempty"`
}

type IssuerPolicy struct {
	Etcd     []IssuerReference `json:"etcd,omitempty"`
	Postgres []IssuerReference `json:"postgres,omitempty"`
	Pgpool   []IssuerReference `json:"pgpool,omitempty"`
}

type StorageClassInventory struct {
	Name                 string   `json:"name"`
	Provisioner          string   `json:"provisioner"`
	ReclaimPolicy        string   `json:"reclaimPolicy,omitempty"`
	VolumeBindingMode    string   `json:"volumeBindingMode,omitempty"`
	AllowVolumeExpansion bool     `json:"allowVolumeExpansion,omitempty"`
	AllowedTopologies    []string `json:"allowedTopologies,omitempty"`
	AccessModes          []string `json:"accessModes,omitempty"`
}

type SiteComponents struct {
	// +kubebuilder:validation:Minimum=0
	EtcdReplicas int32 `json:"etcdReplicas"`
	// +kubebuilder:validation:Minimum=0
	PostgresReplicas int32 `json:"postgresReplicas"`
	// +kubebuilder:validation:Minimum=0
	PgpoolReplicas int32 `json:"pgpoolReplicas"`
}

type StorageRequest struct {
	StorageClassName string            `json:"storageClassName"`
	Size             resource.Quantity `json:"size"`
}

type SiteStorage struct {
	Etcd             *StorageRequest `json:"etcd,omitempty"`
	Postgres         *StorageRequest `json:"postgres,omitempty"`
	RestoreWorkspace *StorageRequest `json:"restoreWorkspace,omitempty"`
}

type LoadBalancerSpec struct {
	AddressPool string `json:"addressPool"`
}

type SecretKeyReference struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

type VaultAuthSpec struct {
	Address           string              `json:"address"`
	AuthMount         string              `json:"authMount"`
	AuthRole          string              `json:"authRole"`
	CABundleSecretRef *SecretKeyReference `json:"caBundleSecretRef,omitempty"`
}

type SiteCertificateSpec struct {
	EtcdIssuerRef     IssuerReference `json:"etcdIssuerRef"`
	PostgresIssuerRef IssuerReference `json:"postgresIssuerRef,omitempty"`
	PgpoolIssuerRef   IssuerReference `json:"pgpoolIssuerRef,omitempty"`
}

type PostgresSiteSpec struct {
	Name                string `json:"name"`
	SiteRegistrationRef string `json:"siteRegistrationRef"`
	Namespace           string `json:"namespace"`
	// +kubebuilder:validation:Enum=Data;Witness
	Role SiteRole `json:"role"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	PrimaryPreference int32               `json:"primaryPreference,omitempty"`
	Components        SiteComponents      `json:"components"`
	Storage           SiteStorage         `json:"storage,omitempty"`
	LoadBalancer      *LoadBalancerSpec   `json:"loadBalancer,omitempty"`
	VaultAuth         *VaultAuthSpec      `json:"vaultAuth,omitempty"`
	Certificates      SiteCertificateSpec `json:"certificates"`
}

type TDEVaultSpec struct {
	KVMount          string `json:"kvMount"`
	KeyPath          string `json:"keyPath"`
	ProviderName     string `json:"providerName"`
	PrincipalKeyName string `json:"principalKeyName"`
}

type TDESpec struct {
	Enabled bool          `json:"enabled,omitempty"`
	Vault   *TDEVaultSpec `json:"vault,omitempty"`
}

type VaultSecretReference struct {
	Mount string `json:"mount"`
	Path  string `json:"path"`
	Key   string `json:"key,omitempty"`
}

type InstanceCredentialsSpec struct {
	PostgresVaultRef VaultSecretReference `json:"postgresVaultRef"`
	PgpoolVaultRef   VaultSecretReference `json:"pgpoolVaultRef,omitempty"`
}

type BackupRepositorySpec struct {
	// +kubebuilder:validation:Enum=S3
	Type     string `json:"type"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix"`
	Endpoint string `json:"endpoint,omitempty"`
	Region   string `json:"region,omitempty"`
	// +kubebuilder:validation:Enum=host;path
	URIStyle           string               `json:"uriStyle,omitempty"`
	CABundleSecretRef  *SecretKeyReference  `json:"caBundleSecretRef,omitempty"`
	CredentialVaultRef VaultSecretReference `json:"credentialVaultRef"`
}

type BackupSchedules struct {
	Full         string `json:"full,omitempty"`
	Differential string `json:"differential,omitempty"`
	Incremental  string `json:"incremental,omitempty"`
	Timezone     string `json:"timezone,omitempty"`
}

type BackupRetention struct {
	Duration    metav1.Duration `json:"duration,omitempty"`
	WALDuration metav1.Duration `json:"walDuration,omitempty"`
}

type BackupSpec struct {
	Repository BackupRepositorySpec `json:"repository"`
	Schedules  BackupSchedules      `json:"schedules,omitempty"`
	Retention  BackupRetention      `json:"retention,omitempty"`
}

type SiteRevisionStatus struct {
	Name                 string             `json:"name"`
	SiteRegistrationRef  string             `json:"siteRegistrationRef"`
	DesiredRevision      int64              `json:"desiredRevision,omitempty"`
	AcknowledgedRevision int64              `json:"acknowledgedRevision,omitempty"`
	AppliedRevision      int64              `json:"appliedRevision,omitempty"`
	Phase                string             `json:"phase,omitempty"`
	Addresses            map[string]string  `json:"addresses,omitempty"`
	Primary              string             `json:"primary,omitempty"`
	SynchronousStandbys  []string           `json:"synchronousStandbys,omitempty"`
	TopologyObservedAt   *metav1.Time       `json:"topologyObservedAt,omitempty"`
	LastHeartbeatTime    *metav1.Time       `json:"lastHeartbeatTime,omitempty"`
	Conditions           []metav1.Condition `json:"conditions,omitempty"`
}

type RoleQuotas struct {
	// +kubebuilder:validation:Minimum=-1
	ConnectionLimit                 *int32             `json:"connectionLimit,omitempty"`
	StatementTimeout                *metav1.Duration   `json:"statementTimeout,omitempty"`
	LockTimeout                     *metav1.Duration   `json:"lockTimeout,omitempty"`
	IdleInTransactionSessionTimeout *metav1.Duration   `json:"idleInTransactionSessionTimeout,omitempty"`
	TempFileLimit                   *resource.Quantity `json:"tempFileLimit,omitempty"`
}

type DatabaseQuotas struct {
	RoleQuotas            `json:",inline"`
	StorageAlertThreshold *resource.Quantity `json:"storageAlertThreshold,omitempty"`
}
