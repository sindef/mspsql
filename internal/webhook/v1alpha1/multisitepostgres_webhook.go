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
var multisitepostgreslog = logf.Log.WithName("multisitepostgres-resource")

// SetupMultiSitePostgresWebhookWithManager registers the webhook for MultiSitePostgres in the manager.
func SetupMultiSitePostgresWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &multisitepostgresv1alpha1.MultiSitePostgres{}).
		WithValidator(&MultiSitePostgresCustomValidator{}).
		WithDefaulter(&MultiSitePostgresCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-multisite-postgres-multisite-postgres-dev-v1alpha1-multisitepostgres,mutating=true,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.dev,resources=multisitepostgres,verbs=create;update,versions=v1alpha1,name=mmultisitepostgres-v1alpha1.kb.io,admissionReviewVersions=v1

// MultiSitePostgresCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind MultiSitePostgres when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type MultiSitePostgresCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind MultiSitePostgres.
func (d *MultiSitePostgresCustomDefaulter) Default(_ context.Context, obj *multisitepostgresv1alpha1.MultiSitePostgres) error {
	multisitepostgreslog.Info("Defaulting for MultiSitePostgres", "name", obj.GetName())

	defaultInstance(obj)
	return nil
}

// +kubebuilder:webhook:path=/validate-multisite-postgres-multisite-postgres-dev-v1alpha1-multisitepostgres,mutating=false,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.dev,resources=multisitepostgres,verbs=create;update,versions=v1alpha1,name=vmultisitepostgres-v1alpha1.kb.io,admissionReviewVersions=v1

// MultiSitePostgresCustomValidator struct is responsible for validating the MultiSitePostgres resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type MultiSitePostgresCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type MultiSitePostgres.
func (v *MultiSitePostgresCustomValidator) ValidateCreate(_ context.Context, obj *multisitepostgresv1alpha1.MultiSitePostgres) (admission.Warnings, error) {
	multisitepostgreslog.Info("Validation for MultiSitePostgres upon creation", "name", obj.GetName())

	return nil, validateInstance(obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type MultiSitePostgres.
func (v *MultiSitePostgresCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *multisitepostgresv1alpha1.MultiSitePostgres) (admission.Warnings, error) {
	multisitepostgreslog.Info("Validation for MultiSitePostgres upon update", "name", newObj.GetName())

	return nil, validateInstance(newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type MultiSitePostgres.
func (v *MultiSitePostgresCustomValidator) ValidateDelete(_ context.Context, obj *multisitepostgresv1alpha1.MultiSitePostgres) (admission.Warnings, error) {
	multisitepostgreslog.Info("Validation for MultiSitePostgres upon deletion", "name", obj.GetName())

	return nil, nil
}
