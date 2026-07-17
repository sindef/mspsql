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

type SiteRegistrationSpec struct {
	DisplayName             string             `json:"displayName,omitempty"`
	Revoked                 bool               `json:"revoked,omitempty"`
	PermittedStorageClasses StorageClassPolicy `json:"permittedStorageClasses"`
	// +listType=map
	// +listMapKey=storageClassName
	StorageRollbackPolicies []StorageRollbackPolicy `json:"storageRollbackPolicies,omitempty"`
	PermittedIssuers        IssuerPolicy            `json:"permittedIssuers"`
	MetallbAddressPools     []string                `json:"metallbAddressPools,omitempty"`
}

type SiteRegistrationStatus struct {
	ObservedGeneration              int64                          `json:"observedGeneration,omitempty"`
	ClusterUID                      string                         `json:"clusterUID,omitempty"`
	Phase                           string                         `json:"phase,omitempty"`
	RegistrationURL                 string                         `json:"registrationURL,omitempty"`
	RegistrationExpiresAt           *metav1.Time                   `json:"registrationExpiresAt,omitempty"`
	LastHeartbeatTime               *metav1.Time                   `json:"lastHeartbeatTime,omitempty"`
	AgentCertificateExpiresAt       *metav1.Time                   `json:"agentCertificateExpiresAt,omitempty"`
	AgentVersion                    string                         `json:"agentVersion,omitempty"`
	DiscoveredStorageClasses        []StorageClassInventory        `json:"discoveredStorageClasses,omitempty"`
	DiscoveredVolumeSnapshotClasses []VolumeSnapshotClassInventory `json:"discoveredVolumeSnapshotClasses,omitempty"`
	DiscoveredIssuers               []IssuerReference              `json:"discoveredIssuers,omitempty"`
	Capabilities                    []string                       `json:"capabilities,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=site
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=".status.agentVersion"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// SiteRegistration is the Schema for the siteregistrations API
type SiteRegistration struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of SiteRegistration
	// +required
	Spec SiteRegistrationSpec `json:"spec"`

	// status defines the observed state of SiteRegistration
	// +optional
	Status SiteRegistrationStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SiteRegistrationList contains a list of SiteRegistration
type SiteRegistrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SiteRegistration `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &SiteRegistration{}, &SiteRegistrationList{})
		return nil
	})
}
