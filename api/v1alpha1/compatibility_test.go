package v1alpha1

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStorageCompatibilityConstants(t *testing.T) {
	if StorageVersion != SchemeGroupVersion.Version {
		t.Fatalf("storage version = %q", StorageVersion)
	}
	if MinimumReadableVersion != SchemeGroupVersion.Version {
		t.Fatalf("minimum readable version = %q", MinimumReadableVersion)
	}
	if SupportedDowngradeLimit == "" {
		t.Fatal("supported downgrade limit is empty")
	}
}

func TestV1Alpha1StorageRoundTripPreservesFields(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC))
	objects := []any{
		&MultiSitePostgres{
			TypeMeta: metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: "MultiSitePostgres"},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "platform", Name: "orders", ResourceVersion: "42",
				Labels: map[string]string{"app": "orders"},
			},
			Spec: MultiSitePostgresSpec{
				DeletionPolicy: DeletionPolicyDelete,
				Postgres: PostgresSpec{
					MajorVersion: 18, Image: "postgres@sha256:abc", SynchronousStandbyCount: 1,
					Parameters: map[string]string{"synchronous_commit": "remote_apply"},
				},
				Sites: []PostgresSiteSpec{{
					Name: "vic", SiteRegistrationRef: "production-vic", Namespace: "orders",
					Role: SiteRoleData, PrimaryPreference: 10,
					Components: SiteComponents{EtcdReplicas: 3, PostgresReplicas: 2, PgpoolReplicas: 2},
					Storage: SiteStorage{
						Etcd: &StorageRequest{StorageClassName: "fast"},
					},
					LoadBalancer: &LoadBalancerSpec{AddressPool: "default"},
					VaultAuth:    &VaultAuthSpec{Address: "https://vault", AuthMount: "k8s", AuthRole: "vic"},
					Certificates: SiteCertificateSpec{
						EtcdIssuerRef: IssuerReference{Name: "etcd"},
					},
				}},
				TDE: TDESpec{Enabled: true, Vault: &TDEVaultSpec{
					KVMount: "mspsql", KeyPath: "orders/tde",
					ProviderName: "vault", PrincipalKeyName: "orders",
				}},
				Backup: &BackupSpec{Repository: BackupRepositorySpec{
					Type: "S3", Bucket: "bucket", Prefix: "orders",
					CredentialVaultRef: VaultSecretReference{Mount: "secret", Path: "backup/orders"},
				}},
				Credentials: InstanceCredentialsSpec{
					PostgresVaultRef: VaultSecretReference{Mount: "secret", Path: "postgres/orders"},
				},
			},
			Status: MultiSitePostgresStatus{
				ObservedGeneration: 3, ActiveRevision: 7, PlanFingerprint: "fp",
				Phase: "Ready", Primary: "postgres-vic-0",
				SynchronousStandbys:        []string{"postgres-vic-1"},
				LastBackupTime:             &now,
				RecoveryWindowStart:        &now,
				RestoreDrillLastVerifiedAt: &now,
				RestoreDrillBackupSet:      "full-20260802",
				BackupSchedules: []BackupScheduleStatus{{
					Type: "full", LastScheduledAt: &now, NextScheduledAt: &now,
					Operation: &OperationProgressStatus{OperationUID: "scheduled-backup", Attempt: 1},
				}},
				Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			},
		},
		&SiteRegistration{
			TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: "SiteRegistration"},
			ObjectMeta: metav1.ObjectMeta{Name: "production-vic"},
			Spec: SiteRegistrationSpec{
				PermittedStorageClasses: StorageClassPolicy{Postgres: []string{"fast"}},
				PermittedIssuers:        IssuerPolicy{Etcd: []IssuerReference{{Name: "etcd"}}},
				StorageRollbackPolicies: []StorageRollbackPolicy{{
					StorageClassName: "fast", Strategy: "PVCClone",
				}},
			},
			Status: SiteRegistrationStatus{
				Phase: "Connected", ClusterUID: "cluster", AgentVersion: "1.2.3",
				Capabilities: []string{"major-upgrade-sync-before-writes"},
				Conditions:   []metav1.Condition{{Type: "Connected", Status: metav1.ConditionTrue}},
			},
		},
		&PostgresRestore{
			TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: "PostgresRestore"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-pitr"},
			Spec: PostgresRestoreSpec{
				SourceInstanceRef: "orders", TargetInstanceRef: "orders-restored",
				TargetTime: now, BackupSet: "full-20260802",
			},
			Status: PostgresRestoreStatus{
				ObservedGeneration: 2, Phase: "Verified", SelectedBackupSet: "full-20260802",
				RecoveredTo: &now, Operation: &OperationProgressStatus{OperationUID: "restore", Attempt: 1},
			},
		},
		&PostgresUpgrade{
			TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: "PostgresUpgrade"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-pg18"},
			Spec: PostgresUpgradeSpec{
				InstanceRef: "orders", TargetImage: "postgres@sha256:def", TargetMajorVersion: 18,
				UpgradeImage: "upgrade@sha256:abc", ServiceRestorationTarget: metav1.Duration{Duration: time.Minute},
				RollbackRetention: metav1.Duration{Duration: time.Hour},
			},
			Status: PostgresUpgradeStatus{
				ObservedGeneration: 2, Phase: "Finalizing", PostUpgradeBackupAttempt: 2,
				Operation: &OperationProgressStatus{OperationUID: "upgrade", Attempt: 2},
			},
		},
		&PostgresDatabase{
			TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: "PostgresDatabase"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-api"},
			Spec: PostgresDatabaseSpec{
				InstanceRef: "orders", DatabaseName: "orders",
				Roles: []DatabaseRole{{Name: "orders_rw"}},
			},
			Status: PostgresDatabaseStatus{
				Operation: &OperationProgressStatus{OperationUID: "database", Attempt: 1},
			},
		},
		&PostgresUser{
			TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: "PostgresUser"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "orders-writer"},
			Spec: PostgresUserSpec{
				InstanceRef: "orders", RoleName: "orders_writer",
				MemberOf:         []RoleMembership{{DatabaseRef: "orders-api", Role: "orders_rw"}},
				PasswordVaultRef: VaultSecretReference{Mount: "secret", Path: "users/orders-writer"},
			},
			Status: PostgresUserStatus{
				Operation: &OperationProgressStatus{OperationUID: "user", Attempt: 1},
			},
		},
	}
	for _, object := range objects {
		t.Run(reflect.TypeOf(object).Elem().Name(), func(t *testing.T) {
			encoded, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			decoded := reflect.New(reflect.TypeOf(object).Elem()).Interface()
			if err := json.Unmarshal(encoded, decoded); err != nil {
				t.Fatal(err)
			}
			encodedAgain, err := json.Marshal(decoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, encodedAgain) {
				t.Fatalf("round trip changed JSON:\nwant %s\ngot  %s", encoded, encodedAgain)
			}
		})
	}
}
