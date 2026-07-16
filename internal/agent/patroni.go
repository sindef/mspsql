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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sindef/mspsql/internal/plan"
)

type PatroniTopology struct {
	Primary             string
	SynchronousStandbys []string
}

type PatroniObserver struct {
	Client client.Client
	HTTP   func(*x509.CertPool) *http.Client
}

func (o *PatroniObserver) Observe(ctx context.Context, desired plan.SitePlan) (PatroniTopology, error) {
	var lastErr error
	for ordinal := int32(0); ordinal < desired.Site.Components.PostgresReplicas; ordinal++ {
		name := fmt.Sprintf("postgres-%s-%d", desired.Site.Name, ordinal)
		topology, err := o.observeMember(ctx, desired.Site.Namespace, name)
		if err == nil {
			return topology, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("site has no PostgreSQL members")
	}
	return PatroniTopology{}, fmt.Errorf("observe Patroni topology: %w", lastErr)
}

func (o *PatroniObserver) observeMember(ctx context.Context, namespace, name string) (PatroniTopology, error) {
	var secret corev1.Secret
	if err := o.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name + "-tls"}, &secret); err != nil {
		return PatroniTopology{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data["ca.crt"]) {
		return PatroniTopology{}, errors.New("PostgreSQL issuer Secret contains no CA certificates")
	}
	httpClient := o.httpClient(roots)
	url := fmt.Sprintf("https://%s.%s.svc:8008/cluster", name, namespace)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return PatroniTopology{}, err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return PatroniTopology{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return PatroniTopology{}, fmt.Errorf("patroni returned HTTP %d", response.StatusCode)
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil {
		return PatroniTopology{}, err
	}
	if len(encoded) > 1024*1024 {
		return PatroniTopology{}, errors.New("patroni response exceeds one MiB")
	}
	var cluster struct {
		Members []struct {
			Name  string `json:"name"`
			Role  string `json:"role"`
			State string `json:"state"`
		} `json:"members"`
	}
	if err := json.Unmarshal(encoded, &cluster); err != nil {
		return PatroniTopology{}, fmt.Errorf("decode Patroni cluster response: %w", err)
	}
	var topology PatroniTopology
	for _, member := range cluster.Members {
		if member.Name == "" {
			continue
		}
		switch member.Role {
		case "leader":
			if member.State == "running" {
				if topology.Primary != "" && topology.Primary != member.Name {
					return PatroniTopology{}, errors.New("patroni reported multiple running leaders")
				}
				topology.Primary = member.Name
			}
		case "sync_standby":
			if member.State == "running" || member.State == "streaming" {
				topology.SynchronousStandbys = append(topology.SynchronousStandbys, member.Name)
			}
		}
	}
	if topology.Primary == "" {
		return PatroniTopology{}, errors.New("patroni reported no running leader")
	}
	slices.Sort(topology.SynchronousStandbys)
	return topology, nil
}

func (o *PatroniObserver) httpClient(roots *x509.CertPool) *http.Client {
	if o.HTTP != nil {
		return o.HTTP(roots)
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
		}},
	}
}
