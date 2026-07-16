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

package vault

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

const maxResponseBytes = 1024 * 1024

type Client struct {
	Address   string
	AuthMount string
	Role      string
	HTTP      *http.Client
}

type Token struct {
	Value     string
	ExpiresAt time.Time
	Renewable bool
}

type Secret struct {
	Data    map[string]string
	Version int64
}

func NewClient(auth api.VaultAuthSpec, caBundle []byte) (*Client, error) {
	httpClient := &http.Client{Timeout: 15 * time.Second}
	if len(caBundle) > 0 {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system CA pool: %w", err)
		}
		if !roots.AppendCertsFromPEM(caBundle) {
			return nil, errors.New("vault CA bundle contains no certificates")
		}
		httpClient.Transport = &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
		}}
	}
	return &Client{
		Address: auth.Address, AuthMount: auth.AuthMount, Role: auth.AuthRole, HTTP: httpClient,
	}, nil
}

func (c *Client) LoginKubernetes(ctx context.Context, jwt string) (Token, error) {
	if strings.TrimSpace(jwt) == "" || c.Role == "" {
		return Token{}, errors.New("vault Kubernetes login requires JWT and role")
	}
	var response struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int64  `json:"lease_duration"`
			Renewable     bool   `json:"renewable"`
		} `json:"auth"`
	}
	if err := c.request(ctx, http.MethodPost, "auth/"+cleanPath(c.AuthMount)+"/login", "",
		map[string]string{"jwt": jwt, "role": c.Role}, &response); err != nil {
		return Token{}, fmt.Errorf("vault Kubernetes login failed: %w", err)
	}
	if response.Auth.ClientToken == "" || response.Auth.LeaseDuration <= 0 {
		return Token{}, errors.New("vault Kubernetes login returned an invalid token lease")
	}
	return Token{
		Value: response.Auth.ClientToken, Renewable: response.Auth.Renewable,
		ExpiresAt: time.Now().Add(time.Duration(response.Auth.LeaseDuration) * time.Second),
	}, nil
}

func (c *Client) ReadKV2(ctx context.Context, token string,
	reference api.VaultSecretReference,
) (Secret, error) {
	if reference.Mount == "" || reference.Path == "" {
		return Secret{}, errors.New("vault KV v2 mount and path are required")
	}
	var response struct {
		Data struct {
			Data     map[string]any `json:"data"`
			Metadata struct {
				Version int64 `json:"version"`
			} `json:"metadata"`
		} `json:"data"`
	}
	resource := cleanPath(reference.Mount) + "/data/" + cleanPath(reference.Path)
	if err := c.request(ctx, http.MethodGet, resource, token, nil, &response); err != nil {
		return Secret{}, fmt.Errorf("read Vault KV v2 secret: %w", err)
	}
	data := make(map[string]string, len(response.Data.Data))
	for key, value := range response.Data.Data {
		text, ok := value.(string)
		if !ok {
			return Secret{}, fmt.Errorf("vault field %q must be a string", key)
		}
		data[key] = text
	}
	if response.Data.Metadata.Version < 1 {
		return Secret{}, errors.New("vault KV v2 response has no metadata version")
	}
	return Secret{Data: data, Version: response.Data.Metadata.Version}, nil
}

func RequireFields(secret Secret, fields ...string) error {
	for _, field := range fields {
		if secret.Data[field] == "" {
			return fmt.Errorf("vault secret is missing non-empty field %q", field)
		}
	}
	return nil
}

func (c *Client) request(ctx context.Context, method, resource, token string, body, output any) error {
	base, err := url.Parse(c.Address)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return errors.New("vault address must be an absolute URL")
	}
	base.Path = path.Join(base.Path, "v1", resource)
	var payload io.Reader
	if body != nil {
		encoded, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return marshalErr
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, base.String(), payload)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("X-Vault-Token", token)
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	limited := io.LimitReader(response.Body, maxResponseBytes+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(encoded) > maxResponseBytes {
		return errors.New("vault response exceeds one MiB")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("vault returned HTTP %d", response.StatusCode)
	}
	if err := json.Unmarshal(encoded, output); err != nil {
		return fmt.Errorf("decode Vault response: %w", err)
	}
	return nil
}

func cleanPath(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}
