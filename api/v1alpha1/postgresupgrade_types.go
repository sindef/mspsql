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

type PostgresUpgradeSpec struct {
	InstanceRef string `json:"instanceRef"`
	TargetImage string `json:"targetImage"`
	// +kubebuilder:validation:Minimum=14
	TargetMajorVersion       int32                  `json:"targetMajorVersion"`
	UpgradeImage             string                 `json:"upgradeImage,omitempty"`
	ServiceRestorationTarget metav1.Duration        `json:"serviceRestorationTarget"`
	RollbackRetention        metav1.Duration        `json:"rollbackRetention,omitempty"`
	Benchmark                *MajorUpgradeBenchmark `json:"benchmark,omitempty"`
}

type MajorUpgradeBenchmark struct {
	TestedAt               metav1.Time     `json:"testedAt"`
	EstimatedWriteOutage   metav1.Duration `json:"estimatedWriteOutage"`
	UpgradeImage           string          `json:"upgradeImage"`
	SourceMajorVersion     int32           `json:"sourceMajorVersion"`
	TargetMajorVersion     int32           `json:"targetMajorVersion"`
	TDEEnabled             bool            `json:"tdeEnabled"`
	PostgresStorageClasses []string        `json:"postgresStorageClasses"`
	Evidence               string          `json:"evidence"`
}

type PostgresUpgradeStatus struct {
	ObservedGeneration           int64        `json:"observedGeneration,omitempty"`
	Phase                        string       `json:"phase,omitempty"`
	StartedAt                    *metav1.Time `json:"startedAt,omitempty"`
	PreflightBackupRequestedAt   *metav1.Time `json:"preflightBackupRequestedAt,omitempty"`
	PostUpgradeBackupRequestedAt *metav1.Time `json:"postUpgradeBackupRequestedAt,omitempty"`
	PostUpgradeBackupAttempt     int32        `json:"postUpgradeBackupAttempt,omitempty"`
	WriteOutageStartedAt         *metav1.Time `json:"writeOutageStartedAt,omitempty"`
	WriteServiceRestoredAt       *metav1.Time `json:"writeServiceRestoredAt,omitempty"`
	UpgradedMembers              []string     `json:"upgradedMembers,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=".spec.instanceRef"
// +kubebuilder:printcolumn:name="Target",type=integer,JSONPath=".spec.targetMajorVersion"

// PostgresUpgrade is the Schema for the postgresupgrades API
type PostgresUpgrade struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PostgresUpgrade
	// +required
	Spec PostgresUpgradeSpec `json:"spec"`

	// status defines the observed state of PostgresUpgrade
	// +optional
	Status PostgresUpgradeStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PostgresUpgradeList contains a list of PostgresUpgrade
type PostgresUpgradeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PostgresUpgrade `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PostgresUpgrade{}, &PostgresUpgradeList{})
		return nil
	})
}
