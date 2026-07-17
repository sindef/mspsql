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
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sindef/mspsql/internal/plan"
)

type Cache struct {
	Client    client.Client
	Namespace string
	PublicKey ed25519.PublicKey
	SiteUID   string
	Now       func() time.Time
}

func (c *Cache) Store(ctx context.Context, envelope plan.Envelope, instanceUID string) (plan.SitePlan, error) {
	current, err := c.Load(ctx, instanceUID)
	minimumRevision := int64(1)
	if err == nil {
		minimumRevision = current.Revision
	} else if !apierrors.IsNotFound(err) {
		return plan.SitePlan{}, err
	}
	desired, err := plan.Verify(c.PublicKey, envelope, c.SiteUID, instanceUID, minimumRevision)
	if err != nil {
		return plan.SitePlan{}, err
	}
	planJSON, err := json.Marshal(envelope.Plan)
	if err != nil {
		return plan.SitePlan{}, err
	}
	name := cacheName(instanceUID)
	data := map[string]string{
		"revision":       fmt.Sprintf("%d", desired.Revision),
		"site-plan.json": string(planJSON),
		"signature":      envelope.Signature,
		"received-at":    c.now().UTC().Format(time.RFC3339Nano),
	}
	var configMap corev1.ConfigMap
	key := client.ObjectKey{Namespace: c.Namespace, Name: name}
	if err := c.Client.Get(ctx, key, &configMap); apierrors.IsNotFound(err) {
		configMap = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: c.Namespace,
				Name:      name,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by":        "mspsql-agent",
					"multisite-postgres.dev/instance-uid": instanceUID,
				},
			},
			Data: data,
		}
		if err := c.Client.Create(ctx, &configMap); err != nil {
			return plan.SitePlan{}, err
		}
		return desired, nil
	} else if err != nil {
		return plan.SitePlan{}, err
	}
	if configMap.Data["revision"] == data["revision"] &&
		configMap.Data["site-plan.json"] == data["site-plan.json"] &&
		configMap.Data["signature"] == data["signature"] {
		return desired, nil
	}
	configMap.Data = data
	if err := c.Client.Update(ctx, &configMap); err != nil {
		return plan.SitePlan{}, err
	}
	return desired, nil
}

func (c *Cache) Load(ctx context.Context, instanceUID string) (plan.SitePlan, error) {
	var configMap corev1.ConfigMap
	if err := c.Client.Get(ctx, client.ObjectKey{
		Namespace: c.Namespace, Name: cacheName(instanceUID),
	}, &configMap); err != nil {
		return plan.SitePlan{}, err
	}
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(configMap.Data["site-plan.json"]), &raw); err != nil {
		return plan.SitePlan{}, fmt.Errorf("decode cached plan: %w", err)
	}
	return plan.Verify(c.PublicKey, plan.Envelope{
		Plan: raw, Signature: configMap.Data["signature"],
	}, c.SiteUID, instanceUID, 1)
}

func (c *Cache) Age(ctx context.Context, instanceUID string) (time.Duration, error) {
	var configMap corev1.ConfigMap
	if err := c.Client.Get(ctx, client.ObjectKey{
		Namespace: c.Namespace, Name: cacheName(instanceUID),
	}, &configMap); err != nil {
		return 0, err
	}
	receivedAt, err := time.Parse(time.RFC3339Nano, configMap.Data["received-at"])
	if err != nil {
		return 0, err
	}
	return c.now().Sub(receivedAt), nil
}

func (c *Cache) List(ctx context.Context) ([]plan.SitePlan, error) {
	var configMaps corev1.ConfigMapList
	if err := c.Client.List(ctx, &configMaps, client.InNamespace(c.Namespace),
		client.MatchingLabels{"app.kubernetes.io/managed-by": "mspsql-agent"}); err != nil {
		return nil, err
	}
	plans := make([]plan.SitePlan, 0, len(configMaps.Items))
	for i := range configMaps.Items {
		instanceUID := configMaps.Items[i].Labels["multisite-postgres.dev/instance-uid"]
		desired, err := c.Load(ctx, instanceUID)
		if err != nil {
			return nil, err
		}
		plans = append(plans, desired)
	}
	return plans, nil
}

func (c *Cache) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func cacheName(instanceUID string) string {
	normalized := strings.ToLower(instanceUID)
	normalized = strings.ReplaceAll(normalized, "_", "-")
	if len(normalized) > 40 {
		normalized = normalized[:40]
	}
	return "multisite-postgres-plan-" + normalized
}
