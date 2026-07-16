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

## Vault contract

Every data site uses `spec.sites[].vaultAuth` to authenticate the
`mspsql-workload` ServiceAccount through Vault's Kubernetes auth method with
the `vault` token audience. Secret references use Vault KV v2 mount and path
names.

For Vault endpoints signed by a private CA, `vaultAuth.caBundleSecretRef`
references a Secret in the site-agent namespace. Its key defaults to `ca.crt`.
The agent uses that trust bundle for Vault API calls and copies only the CA
certificate into the workload's pg_tde runtime Secret.

The referenced records have these required string fields:

| Reference | Fields |
| --- | --- |
| `credentials.postgresVaultRef` | `superuserPassword`, `replicationPassword` |
| `credentials.pgpoolVaultRef` | `adminUsername`, `adminPassword` |
| `backup.repository.credentialVaultRef` | `s3AccessKey`, `s3SecretKey`, `repositoryCipherPassphrase` |
| `PostgresUser.passwordVaultRef` | The field selected by `key` |

When TDE is enabled, `tde.vault.keyPath` is an instance-unique path under the
configured KV v2 mount. `pg_tde` creates and retrieves principal keys there;
the path is not expected to contain a pre-created key value. Vault policies
must grant create/read/update on its data path, read/list on its metadata path,
and read on the mount metadata required by `pg_tde`.

Backup repositories are always encrypted by pgBackRest with the Vault-provided
passphrase. Private S3-compatible endpoints use
`backup.repository.caBundleSecretRef`, which follows the same site-agent
namespace Secret contract as the Vault CA reference.
`sites[].certificates.backupIssuerRef` issues only pgBackRest server and
coordinator identities. Every data site's issuer must publish the same CA
bundle; the hub compares bundle fingerprints and blocks readiness and backup
scheduling on a mismatch.

Major upgrades require `PostgresUpgrade.spec.upgradeImage` pinned by digest.
The image contract provides old binaries under `/opt/mspsql/old/bin`, new
binaries under `/opt/mspsql/new/bin`, their matching library trees under the
corresponding `lib` directories, and `pg_tde_upgrade` on `PATH`. Every
data-site PostgreSQL StorageClass must also have a
`SiteRegistration.spec.storageRollbackPolicies` entry using either a discovered
CSI `VolumeSnapshotClass` or an administrator-asserted PVC clone capability.

## Restore contract

`PostgresRestore` performs time-based PITR into a new instance. Patroni starts
one selected seed member with pgBackRest custom bootstrap, waits for recovery
and promotion, clones the remaining members, establishes synchronous
replication, and only then exposes Pgpool and completes acceptance.

The source repository and historical TDE identity are read-only restore inputs.
`spec.targetBackup`, when supplied, must claim a distinct repository prefix and
Vault credential path and becomes the recovered instance's repository after
promotion. Omitting it leaves the recovered instance without scheduled backups
until its `MultiSitePostgres` backup specification is configured.

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
