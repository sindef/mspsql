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

type RestoreTopology struct {
	Sites []PostgresSiteSpec `json:"sites,omitempty"`
}

type PostgresRestoreSpec struct {
	SourceInstanceRef string          `json:"sourceInstanceRef"`
	TargetInstanceRef string          `json:"targetInstanceRef"`
	TargetTime        metav1.Time     `json:"targetTime"`
	BackupSet         string          `json:"backupSet,omitempty"`
	RestoreTopology   RestoreTopology `json:"restoreTopology,omitempty"`
	TargetBackup      *BackupSpec     `json:"targetBackup,omitempty"`
}

type PostgresRestoreStatus struct {
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
	Phase              string       `json:"phase,omitempty"`
	SelectedBackupSet  string       `json:"selectedBackupSet,omitempty"`
	RecoveredTo        *metav1.Time `json:"recoveredTo,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=".spec.sourceInstanceRef"
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=".spec.targetInstanceRef"

// PostgresRestore is the Schema for the postgresrestores API
type PostgresRestore struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresRestore
	// +required
	Spec PostgresRestoreSpec `json:"spec"`

	// status defines the observed state of PostgresRestore
	// +optional
	Status PostgresRestoreStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresRestoreList contains a list of PostgresRestore
type PostgresRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PostgresRestore{}, &PostgresRestoreList{})
		return nil
	})
}
