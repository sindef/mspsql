# mspsql

`mspsql` is a hub-and-agent Kubernetes operator for independent,
Patroni-managed PostgreSQL installations spanning isolated Kubernetes
clusters.

The hub reconciles `multisite-postgres.dev/v1alpha1` resources in a management
cluster. Site agents make outbound mTLS gRPC connections through a
site-initiated WireGuard tunnel, verify signed desired-state plans, and perform
all target-cluster API operations locally. The hub never receives target
Kubernetes credentials or workload Secret values.

## Components

- `manager`: leader-elected hub controllers, admission webhooks, plan signing,
  registration state, lifecycle serialization, and the agent control service.
- `site-agent`: locally leader-elected plan cache and reconciler.
- `wireguard-go`: a sidecar deployed with each site agent; only the locally
  elected pod activates the tunnel.

The site agent creates member-specific LoadBalancer Services before workloads,
reports allocated addresses to the hub, and waits for a new signed global plan
before configuring etcd membership, certificate SANs, Patroni endpoints, or
Pgpool backends.

## APIs

- `SiteRegistration` is cluster scoped and represents one reusable target
  cluster identity and its permitted platform policy.
- `MultiSitePostgres` declares topology, storage, TLS, TDE, backup, and
  deletion policy.
- `PostgresDatabase` and `PostgresUser` declare non-destructive tenant objects.
- `PostgresRestore` and `PostgresUpgrade` are durable, mutually exclusive
  lifecycle operations.

Deletion defaults to `Retain`. Remote cleanup waits for every connected site;
`multisite-postgres.dev/force-orphan=true` is the explicit escape hatch when an
unreachable site must be orphaned.

## Development

Prerequisites are Go 1.26, Docker, kubectl, and KIND.

```sh
make test
make build
make agent-build
make test-e2e
```

`make test-e2e` creates a management cluster plus VIC, NSW, and QLD site
clusters. It validates site identity binding using each real `kube-system`
namespace UID, policy validation, one signed plan per site, address-driven
revision changes, and blocked/forced finalization.

Build the images with:

```sh
docker build -t registry.example/mspsql:VERSION .
docker build -f Dockerfile.agent -t registry.example/mspsql-agent:VERSION .
```

## Installation

Install cert-manager first, provide the hub workload certificates and
WireGuard gateway configuration, then deploy:

```sh
make install
make deploy IMG=registry.example/mspsql:VERSION
```

Production installations must set `--registration-public-url`, mount the hub
control certificate, key, and client CA under `/etc/mspsql/control`, and
configure the UDP WireGuard LoadBalancer hostname. The target and management
clusters must encrypt Kubernetes Secrets at rest.

Example resources are under `config/samples`. They intentionally contain only
Vault references, never credentials.

## Security invariants

- Plan caches accept only valid Ed25519 signatures, the bound site and
  instance identities, and non-decreasing revisions.
- gRPC requires TLS 1.3 and a client certificate identity matching the
  immutable `SiteRegistration` UID.
- Existing namespaces are never adopted. All four ownership labels must
  already match exactly.
- Disconnected agents do not recreate LoadBalancer Services or apply
  coordinated changes.
- Server-side apply does not force fields owned by Kubernetes, MetalLB,
  cert-manager, or users.
- Secret values are neither included in plans nor returned to the hub.

## Production gates

The repository contains executable control-plane and multi-cluster tests.
Platform-dependent acceptance remains mandatory before rollout: userspace
WireGuard and UDP failover, cross-site etcd trust-chain rotation, MetalLB
address migration under failure, Vault/KMS integration, pgBackRest recovery,
and representative `pg_upgrade`/`pg_tde_upgrade` outage benchmarks. These
cannot be certified by a generic KIND environment.
