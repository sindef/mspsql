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
| `credentials.postgresVaultRef` | `superuserUsername`, `superuserPassword`, `replicationUsername`, `replicationPassword` |
| `credentials.pgpoolVaultRef` | `adminUsername`, `adminPassword` |
| `backup.repository.credentialVaultRef` | `s3AccessKey`, `s3SecretKey`, `repositoryCipherPassphrase` |
| `PostgresUser.passwordVaultRef` | The field selected by `key` |

PostgreSQL infrastructure credential rotation uses alternating roles. The
initial record may use `postgres` and `replication`; later Vault versions must
use distinct `mspsql_admin_*` and `mspsql_replication_*` usernames. Agents
stage the new version without changing live Secrets. The hub creates the new
roles on the observed primary, rolls one synchronous standby and then every
remaining member, disables the previous roles, and finally removes the staged
and previous Secrets. Passwords remain site-local throughout the workflow.

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

Major upgrades require both `targetImage` and `upgradeImage` pinned by digest.
The upgrade image runs as its non-root default user and provides a POSIX shell,
`find`, `grep`, old binaries under `/opt/mspsql/old/bin`, new binaries under
`/opt/mspsql/new/bin`, and their matching library trees under the corresponding
`lib` directories. TDE-qualified images also provide `pg_tde_upgrade` on
`PATH` and `pg_tde.so` in both library trees. The target image provides
`postgres`, `psql`, `pgbackrest`, and `envsubst`.

Every data-site PostgreSQL StorageClass must have a
`SiteRegistration.spec.storageRollbackPolicies` entry using either a discovered
CSI `VolumeSnapshotClass` or an administrator-asserted PVC clone capability.
The operator snapshots or clones every stopped PostgreSQL PVC before conversion
and retains the artifacts for `rollbackRetention`.

`spec.benchmark` is the platform qualification record for the exact upgrade
image, source/target majors, TDE mode, and every PostgreSQL StorageClass used by
the instance. Its evidence reference should identify an immutable CI artifact
containing the disposable-restore benchmark logs and dataset profile. The
record must be at most 30 days old, and its estimated write outage must fit
`serviceRestorationTarget`; otherwise the operator refuses to drain writes.

The state machine requests and verifies a new full backup, drains Pgpool,
stops every member, captures rollback storage, converts only the former
primary, runs offline `pgbackrest stanza-upgrade`, and verifies the target
catalog, TDE audit, and a committed write before restoring Pgpool. Standbys are
then wiped and recloned through Patroni. Completion requires synchronous
topology and another verified full backup. Automatic rollback is permitted only
before Pgpool is restored; afterward the operator preserves new writes and
reports that forward repair is required. Rollback itself restores every PVC,
resets Patroni DCS state, verifies the source version, and only then restores
Pgpool.

Minor upgrades roll one PostgreSQL member at a time. The first target must be
an observed synchronous standby; after its StatefulSet rollout is fully
converged, the agent requests an mTLS-authenticated Patroni switchover and
verifies the new primary before replacing every remaining member. Scheduled
backups pause until the stable target-image plan is applied everywhere.

LoadBalancer address changes are serialized one member per signed revision.
The affected certificate overlaps the old and new SANs. For an etcd voter, a
maintenance Job first proves that a quorum of the other endpoints is healthy,
looks up the member by name, and runs `etcdctl member update` before the
StatefulSet is rolled. The configured etcd image must contain `etcdctl`, a
POSIX shell, `awk`, and `grep`; the default image satisfies this contract.

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
- Setting `SiteRegistration.spec.revoked` is irreversible. It removes bootstrap
  and WireGuard peer credentials and terminates control access without deleting
  workloads in the target cluster.
- The hub owns WireGuard addressing. `--wireguard-network-cidr` supplies a
  dedicated IPv4 network and `--wireguard-endpoint` supplies the public UDP
  `host:port`; registrations receive one persisted `/32`. The generated hub
  identity Secret and peer ConfigMap must be protected by Kubernetes encryption
  at rest and are consumed by the active/passive gateway.
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
