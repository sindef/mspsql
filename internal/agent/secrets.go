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
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"github.com/sindef/mspsql/internal/plan"
	"github.com/sindef/mspsql/internal/vault"
)

const workloadServiceAccount = "mspsql-workload"

type SecretMaterializer struct {
	Client client.Client
	Token  func(context.Context, string, string) (string, error)
	Vault  func(api.VaultAuthSpec) *vault.Client
}

func (m *SecretMaterializer) Reconcile(ctx context.Context, desired plan.SitePlan) error {
	if desired.Site.Role == api.SiteRoleWitness {
		return nil
	}
	if desired.Site.VaultAuth == nil {
		return fmt.Errorf("data site %s has no Vault authentication configuration", desired.Site.Name)
	}
	jwt, err := m.Token(ctx, desired.Site.Namespace, workloadServiceAccount)
	if err != nil {
		return fmt.Errorf("request projected Vault service-account token: %w", err)
	}
	vaultClient := m.Vault(*desired.Site.VaultAuth)
	token, err := vaultClient.LoginKubernetes(ctx, jwt)
	if err != nil {
		return err
	}
	postgres, err := vaultClient.ReadKV2(ctx, token.Value, desired.Credentials.PostgresVaultRef)
	if err != nil {
		return err
	}
	if err := vault.RequireFields(postgres, "superuserPassword", "replicationPassword"); err != nil {
		return fmt.Errorf("postgresql bootstrap credential schema: %w", err)
	}
	if err := m.reconcileSecret(ctx, desired, "postgres-auth", postgres.Version, map[string][]byte{
		"superuser-password":   []byte(postgres.Data["superuserPassword"]),
		"replication-password": []byte(postgres.Data["replicationPassword"]),
	}); err != nil {
		return err
	}
	if desired.Site.Components.PgpoolReplicas > 0 {
		pgpool, readErr := vaultClient.ReadKV2(ctx, token.Value, desired.Credentials.PgpoolVaultRef)
		if readErr != nil {
			return readErr
		}
		if err := vault.RequireFields(pgpool, "adminUsername", "adminPassword"); err != nil {
			return fmt.Errorf("pgpool credential schema: %w", err)
		}
		if err := m.reconcileSecret(ctx, desired, "pgpool-auth", pgpool.Version, map[string][]byte{
			"admin-username": []byte(pgpool.Data["adminUsername"]),
			"admin-password": []byte(pgpool.Data["adminPassword"]),
		}); err != nil {
			return err
		}
	}
	if desired.Backup != nil {
		repository, readErr := vaultClient.ReadKV2(ctx, token.Value,
			desired.Backup.Repository.CredentialVaultRef)
		if readErr != nil {
			return readErr
		}
		if err := vault.RequireFields(repository,
			"s3AccessKey", "s3SecretKey", "repositoryCipherPassphrase"); err != nil {
			return fmt.Errorf("pgBackRest repository credentials: %w", err)
		}
		if err := m.reconcileSecret(ctx, desired, "pgbackrest-repository", repository.Version,
			map[string][]byte{
				"s3-access-key":          []byte(repository.Data["s3AccessKey"]),
				"s3-secret-key":          []byte(repository.Data["s3SecretKey"]),
				"repo-cipher-passphrase": []byte(repository.Data["repositoryCipherPassphrase"]),
			}); err != nil {
			return err
		}
	}
	if desired.TDE.Enabled {
		if desired.TDE.Vault == nil {
			return fmt.Errorf("tde is enabled without a Vault key identity")
		}
		if err := m.reconcileSecret(ctx, desired, "pg-tde-vault", 0, map[string][]byte{
			"token":              []byte(token.Value),
			"token-expires-at":   []byte(token.ExpiresAt.UTC().Format(time.RFC3339)),
			"address":            []byte(desired.Site.VaultAuth.Address),
			"kv-mount":           []byte(desired.TDE.Vault.KVMount),
			"key-path":           []byte(desired.TDE.Vault.KeyPath),
			"provider-name":      []byte(desired.TDE.Vault.ProviderName),
			"principal-key-name": []byte(desired.TDE.Vault.PrincipalKeyName),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *SecretMaterializer) reconcileSecret(ctx context.Context, desired plan.SitePlan, name string,
	version int64, data map[string][]byte,
) error {
	labels := resourceLabels(desired)
	annotations := map[string]string{}
	if version > 0 {
		annotations["multisite-postgres.dev/vault-version"] = strconv.FormatInt(version, 10)
	}
	key := client.ObjectKey{Namespace: desired.Site.Namespace, Name: name}
	var secret corev1.Secret
	if err := m.Client.Get(ctx, key, &secret); apierrors.IsNotFound(err) {
		return m.Client.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: key.Namespace, Name: key.Name, Labels: labels, Annotations: annotations,
			},
			Type: corev1.SecretTypeOpaque,
			Data: data,
		})
	} else if err != nil {
		return err
	}
	secret.Labels = labels
	secret.Annotations = annotations
	secret.Data = data
	secret.Type = corev1.SecretTypeOpaque
	return m.Client.Update(ctx, &secret)
}
