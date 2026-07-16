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

package agent

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	enabledLabel     = "multisite-postgres.dev/enabled"
	hubDomainLabel   = "multisite-postgres.dev/hub-domain"
	siteUIDLabel     = "multisite-postgres.dev/site-registration-uid"
	instanceUIDLabel = "multisite-postgres.dev/instance-uid"
)

var ErrNamespaceOwnershipConflict = errors.New("namespace ownership conflict")

func EnsureNamespace(ctx context.Context, kube client.Client, name, hubDomain, siteUID,
	instanceUID string, connected bool,
) error {
	expected := map[string]string{
		enabledLabel:     "true",
		hubDomainLabel:   hubDomain,
		siteUIDLabel:     siteUID,
		instanceUIDLabel: instanceUID,
	}
	var namespace corev1.Namespace
	if err := kube.Get(ctx, client.ObjectKey{Name: name}, &namespace); apierrors.IsNotFound(err) {
		if !connected {
			return fmt.Errorf("%w: namespace cannot be recreated while disconnected", ErrNamespaceOwnershipConflict)
		}
		namespace = corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: expected},
		}
		return kube.Create(ctx, &namespace)
	} else if err != nil {
		return err
	}
	for key, value := range expected {
		if namespace.Labels[key] != value {
			return fmt.Errorf("%w: label %s must equal %q", ErrNamespaceOwnershipConflict, key, value)
		}
	}
	return nil
}
