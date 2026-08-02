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

package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	directiveStateRunning   = "Running"
	directiveStateSucceeded = "Succeeded"
	directiveStateFailed    = "Failed"
)

type DirectiveState struct {
	OperationUID string
	InstanceUID  string
	Phase        string
	Conditions   []metav1.Condition
}

type DirectiveStateStore interface {
	Load(context.Context, string) (DirectiveState, bool, error)
	MarkRunning(context.Context, string, string) error
	MarkTerminal(context.Context, string, string, []metav1.Condition) error
}

type ConfigMapDirectiveStateStore struct {
	Client    client.Client
	Namespace string
	Now       func() time.Time
}

func (s *ConfigMapDirectiveStateStore) Load(ctx context.Context,
	operationUID string,
) (DirectiveState, bool, error) {
	var configMap corev1.ConfigMap
	if err := s.Client.Get(ctx, client.ObjectKey{
		Namespace: s.Namespace, Name: directiveStateName(operationUID),
	}, &configMap); apierrors.IsNotFound(err) {
		return DirectiveState{}, false, nil
	} else if err != nil {
		return DirectiveState{}, false, err
	}
	state, err := decodeDirectiveState(configMap.Data)
	return state, true, err
}

func (s *ConfigMapDirectiveStateStore) MarkRunning(ctx context.Context,
	operationUID, instanceUID string,
) error {
	return s.upsert(ctx, DirectiveState{
		OperationUID: operationUID, InstanceUID: instanceUID, Phase: directiveStateRunning,
	})
}

func (s *ConfigMapDirectiveStateStore) MarkTerminal(ctx context.Context,
	operationUID, instanceUID string, conditions []metav1.Condition,
) error {
	phase := directiveStateSucceeded
	for _, condition := range conditions {
		if condition.Type == "Succeeded" && condition.Status == metav1.ConditionFalse {
			phase = directiveStateFailed
			break
		}
	}
	return s.upsert(ctx, DirectiveState{
		OperationUID: operationUID, InstanceUID: instanceUID, Phase: phase, Conditions: conditions,
	})
}

func (s *ConfigMapDirectiveStateStore) upsert(ctx context.Context, state DirectiveState) error {
	data, err := encodeDirectiveState(state, s.now())
	if err != nil {
		return err
	}
	key := client.ObjectKey{Namespace: s.Namespace, Name: directiveStateName(state.OperationUID)}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var configMap corev1.ConfigMap
		if err := s.Client.Get(ctx, key, &configMap); apierrors.IsNotFound(err) {
			configMap = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: key.Namespace,
					Name:      key.Name,
					Labels: map[string]string{
						"app.kubernetes.io/managed-by":           "mspsql-agent",
						"multisite-postgres.dev/directive-state": "true",
						"multisite-postgres.dev/instance-uid":    state.InstanceUID,
					},
				},
				Data: data,
			}
			return s.Client.Create(ctx, &configMap)
		} else if err != nil {
			return err
		}
		if configMap.Data["phase"] == directiveStateSucceeded ||
			configMap.Data["phase"] == directiveStateFailed {
			return nil
		}
		if configMap.Labels == nil {
			configMap.Labels = map[string]string{}
		}
		configMap.Labels["app.kubernetes.io/managed-by"] = "mspsql-agent"
		configMap.Labels["multisite-postgres.dev/directive-state"] = "true"
		configMap.Labels["multisite-postgres.dev/instance-uid"] = state.InstanceUID
		configMap.Data = data
		return s.Client.Update(ctx, &configMap)
	})
}

func encodeDirectiveState(state DirectiveState, now time.Time) (map[string]string, error) {
	conditions, err := json.Marshal(state.Conditions)
	if err != nil {
		return nil, fmt.Errorf("encode directive conditions: %w", err)
	}
	return map[string]string{
		"operationUID":    state.OperationUID,
		"instanceUID":     state.InstanceUID,
		"phase":           state.Phase,
		"conditions.json": string(conditions),
		"updated-at":      now.UTC().Format(time.RFC3339Nano),
	}, nil
}

func decodeDirectiveState(data map[string]string) (DirectiveState, error) {
	state := DirectiveState{
		OperationUID: data["operationUID"],
		InstanceUID:  data["instanceUID"],
		Phase:        data["phase"],
	}
	if data["conditions.json"] != "" {
		if err := json.Unmarshal([]byte(data["conditions.json"]), &state.Conditions); err != nil {
			return DirectiveState{}, fmt.Errorf("decode directive conditions: %w", err)
		}
	}
	return state, nil
}

func directiveStateName(operationUID string) string {
	sum := sha256.Sum256([]byte(operationUID))
	return "mspsql-directive-" + hex.EncodeToString(sum[:])[:24]
}

func (s *ConfigMapDirectiveStateStore) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
