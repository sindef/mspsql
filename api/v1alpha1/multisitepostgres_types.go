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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type PostgresSpec struct {
	// +kubebuilder:validation:Minimum=14
	MajorVersion int32  `json:"majorVersion"`
	Image        string `json:"image"`
	// +kubebuilder:validation:Minimum=0
	SynchronousStandbyCount int32             `json:"synchronousStandbyCount"`
	Parameters              map[string]string `json:"parameters,omitempty"`
}

type MultiSitePostgresSpec struct {
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
	Postgres       PostgresSpec   `json:"postgres"`
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	Sites  []PostgresSiteSpec `json:"sites"`
	TDE    TDESpec            `json:"tde,omitempty"`
	Backup *BackupSpec        `json:"backup,omitempty"`
}

type MultiSitePostgresStatus struct {
	ObservedGeneration  int64                `json:"observedGeneration,omitempty"`
	ActiveRevision      int64                `json:"activeRevision,omitempty"`
	PlanFingerprint     string               `json:"planFingerprint,omitempty"`
	Phase               string               `json:"phase,omitempty"`
	Primary             string               `json:"primary,omitempty"`
	SynchronousStandbys []string             `json:"synchronousStandbys,omitempty"`
	Sites               []SiteRevisionStatus `json:"sites,omitempty"`
	LastBackupTime      *metav1.Time         `json:"lastBackupTime,omitempty"`
	RecoveryWindowStart *metav1.Time         `json:"recoveryWindowStart,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Revision",type=integer,JSONPath=".status.activeRevision"
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=".status.primary"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// MultiSitePostgres is the Schema for the multisitepostgres API
type MultiSitePostgres struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of MultiSitePostgres
	// +required
	Spec MultiSitePostgresSpec `json:"spec"`

	// status defines the observed state of MultiSitePostgres
	// +optional
	Status MultiSitePostgresStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// MultiSitePostgresList contains a list of MultiSitePostgres
type MultiSitePostgresList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []MultiSitePostgres `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &MultiSitePostgres{}, &MultiSitePostgresList{})
		return nil
	})
}
