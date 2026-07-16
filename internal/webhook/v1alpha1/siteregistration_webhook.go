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

	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multisitepostgresv1alpha1 "github.com/sindef/mspsql/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var siteregistrationlog = logf.Log.WithName("siteregistration-resource")

// SetupSiteRegistrationWebhookWithManager registers the webhook for SiteRegistration in the manager.
func SetupSiteRegistrationWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &multisitepostgresv1alpha1.SiteRegistration{}).
		WithValidator(&SiteRegistrationCustomValidator{}).
		WithDefaulter(&SiteRegistrationCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-multisite-postgres-multisite-postgres-dev-v1alpha1-siteregistration,mutating=true,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.multisite-postgres.dev,resources=siteregistrations,verbs=create;update,versions=v1alpha1,name=msiteregistration-v1alpha1.kb.io,admissionReviewVersions=v1

// SiteRegistrationCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind SiteRegistration when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type SiteRegistrationCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind SiteRegistration.
func (d *SiteRegistrationCustomDefaulter) Default(_ context.Context, obj *multisitepostgresv1alpha1.SiteRegistration) error {
	siteregistrationlog.Info("Defaulting for SiteRegistration", "name", obj.GetName())

	return nil
}

// +kubebuilder:webhook:path=/validate-multisite-postgres-multisite-postgres-dev-v1alpha1-siteregistration,mutating=false,failurePolicy=fail,sideEffects=None,groups=multisite-postgres.multisite-postgres.dev,resources=siteregistrations,verbs=create;update,versions=v1alpha1,name=vsiteregistration-v1alpha1.kb.io,admissionReviewVersions=v1

// SiteRegistrationCustomValidator struct is responsible for validating the SiteRegistration resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type SiteRegistrationCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type SiteRegistration.
func (v *SiteRegistrationCustomValidator) ValidateCreate(_ context.Context, obj *multisitepostgresv1alpha1.SiteRegistration) (admission.Warnings, error) {
	siteregistrationlog.Info("Validation for SiteRegistration upon creation", "name", obj.GetName())

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type SiteRegistration.
func (v *SiteRegistrationCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj *multisitepostgresv1alpha1.SiteRegistration) (admission.Warnings, error) {
	siteregistrationlog.Info("Validation for SiteRegistration upon update", "name", newObj.GetName())

	if oldObj.Status.ClusterUID != "" && newObj.Status.ClusterUID != oldObj.Status.ClusterUID {
		return nil, field.Forbidden(field.NewPath("status", "clusterUID"), "cluster binding is immutable")
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type SiteRegistration.
func (v *SiteRegistrationCustomValidator) ValidateDelete(_ context.Context, obj *multisitepostgresv1alpha1.SiteRegistration) (admission.Warnings, error) {
	siteregistrationlog.Info("Validation for SiteRegistration upon deletion", "name", obj.GetName())

	return nil, nil
}
