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

package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const signingKeySecretName = "mspsql-plan-signing-key"

func ensureSigningKey(ctx context.Context, kube client.Client, namespace string) (ed25519.PrivateKey, error) {
	key := types.NamespacedName{Namespace: namespace, Name: signingKeySecretName}
	var secret corev1.Secret
	if err := kube.Get(ctx, key, &secret); err == nil {
		privateKey, err := base64.RawStdEncoding.DecodeString(string(secret.Data["privateKey"]))
		if err != nil || len(privateKey) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("plan signing Secret contains an invalid private key")
		}
		return ed25519.PrivateKey(privateKey), nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	immutable := true
	secret = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: signingKeySecretName},
		Immutable:  &immutable,
		Data: map[string][]byte{
			"privateKey": []byte(base64.RawStdEncoding.EncodeToString(privateKey)),
			"publicKey":  []byte(base64.RawStdEncoding.EncodeToString(publicKey)),
		},
	}
	if err := kube.Create(ctx, &secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return ensureSigningKey(ctx, kube, namespace)
		}
		return nil, err
	}
	return privateKey, nil
}

func envelopeData(value any) (map[string]string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return map[string]string{"envelope.json": string(encoded)}, nil
}
