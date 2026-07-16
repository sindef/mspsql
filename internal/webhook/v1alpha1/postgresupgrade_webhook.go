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
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var postgresupgradelog = logf.Log.WithName("postgresupgrade-resource")

// SetupPostgresUpgradeWebhookWithManager registers the webhook for PostgresUpgrade in the manager.
func SetupPostgresUpgradeWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &multisitepostgresv1alpha1.PostgresUpgrade{}).
		WithValidator(&PostgresUpgradeCustomValidator{}).
		WithDefaulter(&PostgresUpgradeCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-multisite-postgres-multisite-postgres-dev-v1alpha1-postgresupgrade,mutating=true,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.multisite-postgres.dev,resources=postgresupgrades,verbs=create;update,versions=v1alpha1,name=mpostgresupgrade-v1alpha1.kb.io,admissionReviewVersions=v1

// PostgresUpgradeCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind PostgresUpgrade when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type PostgresUpgradeCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind PostgresUpgrade.
func (d *PostgresUpgradeCustomDefaulter) Default(_ context.Context, obj *multisitepostgresv1alpha1.PostgresUpgrade) error {
	postgresupgradelog.Info("Defaulting for PostgresUpgrade", "name", obj.GetName())

	if obj.Spec.ServiceRestorationTarget.Duration == 0 {
		obj.Spec.ServiceRestorationTarget.Duration = 15 * time.Minute
	}
	return nil
}

// +kubebuilder:webhook:path=/validate-multisite-postgres-multisite-postgres-dev-v1alpha1-postgresupgrade,mutating=false,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.multisite-postgres.dev,resources=postgresupgrades,verbs=create;update,versions=v1alpha1,name=vpostgresupgrade-v1alpha1.kb.io,admissionReviewVersions=v1

// PostgresUpgradeCustomValidator struct is responsible for validating the PostgresUpgrade resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type PostgresUpgradeCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type PostgresUpgrade.
func (v *PostgresUpgradeCustomValidator) ValidateCreate(_ context.Context, obj *multisitepostgresv1alpha1.PostgresUpgrade) (admission.Warnings, error) {
	postgresupgradelog.Info("Validation for PostgresUpgrade upon creation", "name", obj.GetName())

	return nil, validateUpgrade(obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type PostgresUpgrade.
func (v *PostgresUpgradeCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *multisitepostgresv1alpha1.PostgresUpgrade) (admission.Warnings, error) {
	postgresupgradelog.Info("Validation for PostgresUpgrade upon update", "name", newObj.GetName())

	return nil, validateUpgrade(newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type PostgresUpgrade.
func (v *PostgresUpgradeCustomValidator) ValidateDelete(_ context.Context, obj *multisitepostgresv1alpha1.PostgresUpgrade) (admission.Warnings, error) {
	postgresupgradelog.Info("Validation for PostgresUpgrade upon deletion", "name", obj.GetName())

	return nil, nil
}
