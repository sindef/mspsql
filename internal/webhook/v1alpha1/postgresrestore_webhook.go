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

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var postgresrestorelog = logf.Log.WithName("postgresrestore-resource")

// SetupPostgresRestoreWebhookWithManager registers the webhook for PostgresRestore in the manager.
func SetupPostgresRestoreWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &multisitepostgresv1alpha1.PostgresRestore{}).
		WithValidator(&PostgresRestoreCustomValidator{}).
		WithDefaulter(&PostgresRestoreCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-multisite-postgres-multisite-postgres-dev-v1alpha1-postgresrestore,mutating=true,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.multisite-postgres.dev,resources=postgresrestores,verbs=create;update,versions=v1alpha1,name=mpostgresrestore-v1alpha1.kb.io,admissionReviewVersions=v1

// PostgresRestoreCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind PostgresRestore when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type PostgresRestoreCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind PostgresRestore.
func (d *PostgresRestoreCustomDefaulter) Default(_ context.Context, obj *multisitepostgresv1alpha1.PostgresRestore) error {
	postgresrestorelog.Info("Defaulting for PostgresRestore", "name", obj.GetName())

	return nil
}

// +kubebuilder:webhook:path=/validate-multisite-postgres-multisite-postgres-dev-v1alpha1-postgresrestore,mutating=false,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.multisite-postgres.dev,resources=postgresrestores,verbs=create;update,versions=v1alpha1,name=vpostgresrestore-v1alpha1.kb.io,admissionReviewVersions=v1

// PostgresRestoreCustomValidator struct is responsible for validating the PostgresRestore resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type PostgresRestoreCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type PostgresRestore.
func (v *PostgresRestoreCustomValidator) ValidateCreate(_ context.Context, obj *multisitepostgresv1alpha1.PostgresRestore) (admission.Warnings, error) {
	postgresrestorelog.Info("Validation for PostgresRestore upon creation", "name", obj.GetName())

	return nil, validateRestore(obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type PostgresRestore.
func (v *PostgresRestoreCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *multisitepostgresv1alpha1.PostgresRestore) (admission.Warnings, error) {
	postgresrestorelog.Info("Validation for PostgresRestore upon update", "name", newObj.GetName())

	return nil, validateRestore(newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type PostgresRestore.
func (v *PostgresRestoreCustomValidator) ValidateDelete(_ context.Context, obj *multisitepostgresv1alpha1.PostgresRestore) (admission.Warnings, error) {
	postgresrestorelog.Info("Validation for PostgresRestore upon deletion", "name", obj.GetName())

	return nil, nil
}
