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

type RoleMembership struct {
	DatabaseRef string `json:"databaseRef"`
	Role        string `json:"role"`
}

type PostgresUserSpec struct {
	InstanceRef string `json:"instanceRef"`
	RoleName    string `json:"roleName"`
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	DeletionPolicy   DeletionPolicy       `json:"deletionPolicy,omitempty"`
	MemberOf         []RoleMembership     `json:"memberOf,omitempty"`
	PasswordVaultRef VaultSecretReference `json:"passwordVaultRef"`
	Quotas           RoleQuotas           `json:"quotas,omitempty"`
}

type PostgresUserStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Phase              string `json:"phase,omitempty"`
	CredentialVersion  int64  `json:"credentialVersion,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=".spec.roleName"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// PostgresUser is the Schema for the postgresusers API
type PostgresUser struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresUser
	// +required
	Spec PostgresUserSpec `json:"spec"`

	// status defines the observed state of PostgresUser
	// +optional
	Status PostgresUserStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresUserList contains a list of PostgresUser
type PostgresUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PostgresUser{}, &PostgresUserList{})
		return nil
	})
}
