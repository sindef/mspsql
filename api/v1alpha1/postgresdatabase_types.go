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
	"k8s.io/apimachinery/pkg/runtime"
)

type DatabaseRole struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=Owner;ReadWrite;ReadOnly
	Profile string `json:"profile"`
}

type PostgresDatabaseSpec struct {
	InstanceRef  string `json:"instanceRef"`
	DatabaseName string `json:"databaseName"`
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
	Schemas        []string       `json:"schemas,omitempty"`
	Roles          []DatabaseRole `json:"roles,omitempty"`
	Quotas         DatabaseQuotas `json:"quotas,omitempty"`
}

type PostgresDatabaseStatus struct {
	ObservedGeneration   int64             `json:"observedGeneration,omitempty"`
	Phase                string            `json:"phase,omitempty"`
	TDEVerified          bool              `json:"tdeVerified,omitempty"`
	ObservedSize         resource.Quantity `json:"observedSize,omitempty"`
	OrphanedDeclarations []string          `json:"orphanedDeclarations,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=".spec.databaseName"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// PostgresDatabase is the Schema for the postgresdatabases API
type PostgresDatabase struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresDatabase
	// +required
	Spec PostgresDatabaseSpec `json:"spec"`

	// status defines the observed state of PostgresDatabase
	// +optional
	Status PostgresDatabaseStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresDatabaseList contains a list of PostgresDatabase
type PostgresDatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresDatabase `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PostgresDatabase{}, &PostgresDatabaseList{})
		return nil
	})
}
