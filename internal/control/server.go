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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"github.com/sindef/mspsql/internal/directive"
	"github.com/sindef/mspsql/internal/plan"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	controlv1.UnimplementedAgentControlServer
	Client          client.Client
	Now             func() time.Time
	SystemNamespace string
	SignCertificate func(context.Context, *api.SiteRegistration, []byte) ([]byte, []byte, error)
}

type lockedControlStream struct {
	controlv1.AgentControl_ConnectServer
	sendMu sync.Mutex
}

func (s *lockedControlStream) Send(message *controlv1.HubMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.AgentControl_ConnectServer.Send(message)
}

func (s *Server) Connect(stream controlv1.AgentControl_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first message must be AgentHello")
	}
	if hello.ProtocolVersion != plan.ProtocolVersion {
		return status.Errorf(codes.FailedPrecondition, "unsupported protocol version %q", hello.ProtocolVersion)
	}
	peerCertificate, err := validatePeerIdentity(stream.Context(), hello.RegistrationUid)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	site, err := s.bindSite(stream.Context(), hello, peerCertificate.NotAfter)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	lockedStream := &lockedControlStream{AgentControl_ConnectServer: stream}
	errs := make(chan error, 2)
	go func() { errs <- s.sendPlans(ctx, lockedStream, site.Name, string(site.UID)) }()
	go func() { errs <- s.receive(ctx, lockedStream, site) }()
	err = <-errs
	cancel()
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *Server) bindSite(ctx context.Context, hello *controlv1.AgentHello,
	certificateNotAfter time.Time,
) (*api.SiteRegistration, error) {
	var sites api.SiteRegistrationList
	if err := s.Client.List(ctx, &sites); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for i := range sites.Items {
		site := &sites.Items[i]
		if string(site.UID) != hello.RegistrationUid {
			continue
		}
		if site.Spec.Revoked {
			return nil, status.Error(codes.PermissionDenied, "site registration is revoked")
		}
		updated, err := s.updateSiteStatus(ctx, site.Name, func(current *api.SiteRegistration) error {
			if current.Status.ClusterUID != "" && current.Status.ClusterUID != hello.ClusterUid {
				return status.Error(codes.PermissionDenied,
					"registration is permanently bound to another Kubernetes cluster UID")
			}
			now := metav1.NewTime(s.now())
			current.Status.ClusterUID = hello.ClusterUid
			current.Status.Phase = "Connected"
			current.Status.AgentVersion = hello.AgentVersion
			current.Status.Capabilities = append([]string(nil), hello.Capabilities...)
			current.Status.LastHeartbeatTime = &now
			expiresAt := metav1.NewTime(certificateNotAfter)
			current.Status.AgentCertificateExpiresAt = &expiresAt
			setSiteCondition(&current.Status.Conditions, "Connected", metav1.ConditionTrue,
				"ControlStreamEstablished", "The authenticated agent control stream is active")
			setSiteCondition(&current.Status.Conditions, "IdentityReady", metav1.ConditionTrue,
				"CertificateAuthenticated", "The agent presented a valid site identity certificate")
			return nil
		})
		if err != nil {
			if status.Code(err) != codes.Unknown {
				return nil, err
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		return updated, nil
	}
	return nil, status.Error(codes.NotFound, "site registration UID was not found")
}

func (s *Server) sendPlans(ctx context.Context, stream controlv1.AgentControl_ConnectServer,
	siteName, siteUID string,
) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	sent := map[string]int64{}
	sentDirectives := map[string]struct{}{}
	for {
		if err := s.ensureSiteActive(ctx, siteName, siteUID); err != nil {
			return err
		}
		var configMaps corev1.ConfigMapList
		if err := s.Client.List(ctx, &configMaps, client.MatchingLabels{
			"multisite-postgres.dev/site-registration-uid": siteUID,
		}); err != nil {
			return err
		}
		for i := range configMaps.Items {
			configMap := &configMaps.Items[i]
			var envelope plan.Envelope
			if err := json.Unmarshal([]byte(configMap.Data["envelope.json"]), &envelope); err != nil {
				return fmt.Errorf("decode plan ConfigMap %s: %w", configMap.Name, err)
			}
			var desired plan.SitePlan
			if err := json.Unmarshal(envelope.Plan, &desired); err != nil {
				return fmt.Errorf("decode desired plan %s: %w", configMap.Name, err)
			}
			if sent[desired.InstanceUID] >= desired.Revision {
				continue
			}
			encoded, err := json.Marshal(envelope)
			if err != nil {
				return err
			}
			if err := stream.Send(&controlv1.HubMessage{
				Message: &controlv1.HubMessage_Plan{Plan: &controlv1.DesiredSitePlan{
					InstanceUid: desired.InstanceUID, Revision: desired.Revision, EnvelopeJson: encoded,
				}},
			}); err != nil {
				return err
			}
			sent[desired.InstanceUID] = desired.Revision
		}
		if err := s.sendDirectives(ctx, stream, siteName, siteUID, sentDirectives); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) ensureSiteActive(ctx context.Context, name, uid string) error {
	var site api.SiteRegistration
	if err := s.Client.Get(ctx, client.ObjectKey{Name: name}, &site); err != nil {
		if apierrors.IsNotFound(err) {
			return status.Error(codes.PermissionDenied, "site registration no longer exists")
		}
		return err
	}
	if string(site.UID) != uid || site.Spec.Revoked {
		return status.Error(codes.PermissionDenied, "site registration is revoked")
	}
	return nil
}

func (s *Server) sendDirectives(ctx context.Context, stream controlv1.AgentControl_ConnectServer,
	siteName, siteUID string, sent map[string]struct{},
) error {
	var configMaps corev1.ConfigMapList
	if err := s.Client.List(ctx, &configMaps, client.HasLabels{
		"multisite-postgres.dev/directive",
	}); err != nil {
		return err
	}
	var privateKey ed25519.PrivateKey
	for i := range configMaps.Items {
		configMap := &configMaps.Items[i]
		operationUID := configMap.Data["operationUID"]
		if _, exists := sent[operationUID]; operationUID == "" || exists {
			continue
		}
		var instance api.MultiSitePostgres
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: configMap.Namespace, Name: configMap.Data["instanceRef"],
		}, &instance); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		directiveType := configMap.Data["type"]
		backupSource := ""
		backupType := ""
		if directiveType == "Backup" {
			var backupSpec struct {
				BackupType string `json:"backupType"`
			}
			if err := json.Unmarshal([]byte(configMap.Data["spec.json"]), &backupSpec); err != nil {
				return fmt.Errorf("decode backup directive %s: %w", configMap.Name, err)
			}
			backupType = backupSpec.BackupType
			backupSource = selectBackupSource(&instance)
			if backupSource == "" || !memberTargetsSite(&instance, backupSource, siteName) {
				continue
			}
		} else if !directiveTargetsSite(&instance, directiveType, siteName) {
			continue
		}
		owner := metav1.GetControllerOf(configMap)
		trusted, trustErr := s.directiveOwnerTrusted(ctx, configMap, owner, &instance)
		if trustErr != nil {
			return trustErr
		}
		if !trusted {
			continue
		}
		if privateKey == nil {
			loaded, loadErr := s.signingKey(ctx)
			if loadErr != nil {
				return loadErr
			}
			privateKey = loaded
		}
		envelope, err := directive.Sign(privateKey, directive.Payload{
			SiteUID: siteUID, InstanceUID: string(instance.UID), OperationUID: operationUID,
			ObjectUID: string(owner.UID),
			Type:      directiveType, Primary: instance.Status.Primary,
			BackupSource: backupSource, BackupType: backupType,
			Deleting: configMap.Data["deleting"] == "true", GeneratedAt: s.now().UTC(),
			Spec: json.RawMessage(configMap.Data["spec.json"]),
		})
		if err != nil {
			return fmt.Errorf("sign directive %s: %w", configMap.Name, err)
		}
		encoded, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		if err := stream.Send(&controlv1.HubMessage{
			Message: &controlv1.HubMessage_Directive{Directive: &controlv1.OperationDirective{
				OperationUid: operationUID, InstanceUid: string(instance.UID),
				Type: directiveType, DirectiveJson: encoded,
			}},
		}); err != nil {
			return err
		}
		sent[operationUID] = struct{}{}
	}
	return nil
}

func (s *Server) directiveOwnerTrusted(ctx context.Context, configMap *corev1.ConfigMap,
	owner *metav1.OwnerReference, instance *api.MultiSitePostgres,
) (bool, error) {
	if owner == nil || owner.APIVersion != api.GroupVersion.String() || owner.UID == "" ||
		configMap.Data["instanceRef"] != instance.Name {
		return false, nil
	}
	switch owner.Kind {
	case "PostgresDatabase":
		return s.databaseDirectiveTrusted(ctx, configMap, owner, instance)
	case "PostgresUser":
		return s.userDirectiveTrusted(ctx, configMap, owner, instance)
	case "MultiSitePostgres":
		return scheduledBackupDirectiveTrusted(configMap, owner, instance), nil
	case "PostgresUpgrade":
		return s.upgradeBackupDirectiveTrusted(ctx, configMap, owner, instance)
	default:
		return false, nil
	}
}

func (s *Server) databaseDirectiveTrusted(ctx context.Context, configMap *corev1.ConfigMap,
	owner *metav1.OwnerReference, instance *api.MultiSitePostgres,
) (bool, error) {
	if configMap.Data["type"] != "Database" || configMap.Name != "mspsql-database-"+owner.Name {
		return false, nil
	}
	var object api.PostgresDatabase
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: configMap.Namespace, Name: owner.Name},
		&object); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return declarationDirectiveTrusted(configMap, owner, instance, object.UID, object.Generation,
		object.Spec.InstanceRef, object.DeletionTimestamp, object.Spec.DeletionPolicy, object.Spec)
}

func (s *Server) userDirectiveTrusted(ctx context.Context, configMap *corev1.ConfigMap,
	owner *metav1.OwnerReference, instance *api.MultiSitePostgres,
) (bool, error) {
	if configMap.Data["type"] != "User" || configMap.Name != "mspsql-user-"+owner.Name {
		return false, nil
	}
	var object api.PostgresUser
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: configMap.Namespace, Name: owner.Name},
		&object); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return declarationDirectiveTrusted(configMap, owner, instance, object.UID, object.Generation,
		object.Spec.InstanceRef, object.DeletionTimestamp, object.Spec.DeletionPolicy, object.Spec)
}

func declarationDirectiveTrusted(configMap *corev1.ConfigMap, owner *metav1.OwnerReference,
	instance *api.MultiSitePostgres, uid types.UID, generation int64, instanceRef string,
	deletionTimestamp *metav1.Time, deletionPolicy api.DeletionPolicy, spec any,
) (bool, error) {
	deleting := configMap.Data["deleting"] == "true"
	if uid != owner.UID || instanceRef != instance.Name ||
		configMap.Data["operationUID"] != fmt.Sprintf("%s-%d-%t", uid, generation, deleting) ||
		deleting != (deletionTimestamp != nil && !deletionTimestamp.IsZero()) ||
		deleting && deletionPolicy != api.DeletionPolicyDelete {
		return false, nil
	}
	encoded, err := json.Marshal(spec)
	return bytes.Equal(encoded, []byte(configMap.Data["spec.json"])), err
}

func scheduledBackupDirectiveTrusted(configMap *corev1.ConfigMap, owner *metav1.OwnerReference,
	instance *api.MultiSitePostgres,
) bool {
	if configMap.Data["type"] != "Backup" || owner.UID != instance.UID ||
		owner.Name != instance.Name || configMap.Data["deleting"] != "false" {
		return false
	}
	var scheduled struct {
		BackupType  string `json:"backupType"`
		ScheduledAt string `json:"scheduledAt"`
	}
	if json.Unmarshal([]byte(configMap.Data["spec.json"]), &scheduled) != nil {
		return false
	}
	scheduledAt, err := time.Parse(time.RFC3339, scheduled.ScheduledAt)
	if err != nil {
		return false
	}
	for _, scheduleStatus := range instance.Status.BackupSchedules {
		if scheduleStatus.Type == scheduled.BackupType && scheduleStatus.LastScheduledAt != nil &&
			scheduleStatus.LastScheduledAt.Equal(&metav1.Time{Time: scheduledAt}) {
			expectedName := fmt.Sprintf("mspsql-backup-%s-%d", scheduled.BackupType, scheduledAt.Unix())
			expectedOperation := fmt.Sprintf("%s-backup-%s-%d", instance.UID,
				scheduled.BackupType, scheduledAt.Unix())
			return configMap.Name == expectedName && configMap.Data["operationUID"] == expectedOperation
		}
	}
	return false
}

func (s *Server) upgradeBackupDirectiveTrusted(ctx context.Context, configMap *corev1.ConfigMap,
	owner *metav1.OwnerReference, instance *api.MultiSitePostgres,
) (bool, error) {
	if configMap.Data["type"] != "Backup" || configMap.Data["deleting"] != "false" {
		return false, nil
	}
	var object api.PostgresUpgrade
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: configMap.Namespace, Name: owner.Name},
		&object); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if object.UID != owner.UID || object.Spec.InstanceRef != instance.Name {
		return false, nil
	}
	switch configMap.Data["upgradeBackupPhase"] {
	case "preflight":
		return object.Status.PreflightBackupRequestedAt != nil &&
			configMap.Name == "mspsql-upgrade-backup-"+string(object.UID) &&
			configMap.Data["operationUID"] == string(object.UID)+"-preflight-backup", nil
	case "post-upgrade":
		return object.Status.PostUpgradeBackupRequestedAt != nil &&
			configMap.Name == fmt.Sprintf("mspsql-post-upgrade-backup-%s-%d",
				object.UID, object.Status.PostUpgradeBackupAttempt) &&
			configMap.Data["operationUID"] == fmt.Sprintf("%s-post-upgrade-backup-%d",
				object.UID, object.Status.PostUpgradeBackupAttempt), nil
	default:
		return false, nil
	}
}

func selectBackupSource(instance *api.MultiSitePostgres) string {
	standbys := slices.Clone(instance.Status.SynchronousStandbys)
	slices.Sort(standbys)
	for _, standby := range standbys {
		if memberSite(instance, standby) != "" {
			return standby
		}
	}
	if memberSite(instance, instance.Status.Primary) != "" {
		return instance.Status.Primary
	}
	return ""
}

func memberTargetsSite(instance *api.MultiSitePostgres, member, siteName string) bool {
	targetSite := memberSite(instance, member)
	for _, desired := range instance.Spec.Sites {
		if desired.Name == targetSite {
			return desired.SiteRegistrationRef == siteName
		}
	}
	return false
}

func memberSite(instance *api.MultiSitePostgres, member string) string {
	for _, observed := range instance.Status.Sites {
		if _, found := observed.Addresses[member]; found {
			return observed.Name
		}
	}
	return ""
}

func directiveTargetsSite(instance *api.MultiSitePostgres, directiveType, siteName string) bool {
	if directiveType != "Database" && directiveType != "User" {
		return true
	}
	if instance.Status.Primary == "" {
		return false
	}
	for _, observed := range instance.Status.Sites {
		if _, found := observed.Addresses[instance.Status.Primary]; !found {
			continue
		}
		for _, desired := range instance.Spec.Sites {
			if desired.Name == observed.Name {
				return desired.SiteRegistrationRef == siteName
			}
		}
	}
	return false
}

func (s *Server) signingKey(ctx context.Context) (ed25519.PrivateKey, error) {
	namespace := s.SystemNamespace
	if namespace == "" {
		namespace = "mspsql-system"
	}
	var secret corev1.Secret
	if err := s.Client.Get(ctx, client.ObjectKey{
		Namespace: namespace, Name: "mspsql-plan-signing-key",
	}, &secret); err != nil {
		return nil, err
	}
	privateKey, err := base64.RawStdEncoding.DecodeString(string(secret.Data["privateKey"]))
	if err != nil || len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("plan signing Secret contains an invalid private key")
	}
	return ed25519.PrivateKey(privateKey), nil
}

func (s *Server) receive(ctx context.Context, stream controlv1.AgentControl_ConnectServer,
	site *api.SiteRegistration,
) error {
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case message.GetCertificateSigningRequest() != nil:
			if err := s.rotateCertificate(ctx, stream, site,
				message.GetCertificateSigningRequest()); err != nil {
				return err
			}
		case message.GetHeartbeat() != nil:
			if err := s.recordHeartbeat(ctx, site.Name, message.GetHeartbeat()); err != nil {
				return err
			}
		case message.GetAcknowledgement() != nil:
			if err := s.recordAcknowledgement(ctx, site.Name, message.GetAcknowledgement()); err != nil {
				return err
			}
		case message.GetProgress() != nil:
			if err := s.recordProgress(ctx, site.Name, message.GetProgress()); err != nil {
				return err
			}
		case message.GetResult() != nil:
			if err := s.recordResult(ctx, site.Name, message.GetResult()); err != nil {
				return err
			}
		case message.GetInventory() != nil:
			if err := s.recordInventory(ctx, site.Name, message.GetInventory()); err != nil {
				return err
			}
		}
	}
}

func (s *Server) rotateCertificate(ctx context.Context, stream controlv1.AgentControl_ConnectServer,
	site *api.SiteRegistration, request *controlv1.CertificateSigningRequest,
) error {
	if s.SignCertificate == nil {
		return status.Error(codes.FailedPrecondition, "certificate renewal is not configured")
	}
	if request.RequestId == "" || len(request.RequestId) > 128 ||
		len(request.CsrPem) == 0 || len(request.CsrPem) > 16<<10 {
		return status.Error(codes.InvalidArgument, "certificate signing request is invalid")
	}
	if err := s.ensureSiteActive(ctx, site.Name, string(site.UID)); err != nil {
		return err
	}
	certificatePEM, caPEM, err := s.SignCertificate(ctx, site, request.CsrPem)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	block, _ := pem.Decode(certificatePEM)
	if block == nil {
		return status.Error(codes.Internal, "certificate signer returned invalid PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return status.Error(codes.Internal, "certificate signer returned an invalid certificate")
	}
	if err := stream.Send(&controlv1.HubMessage{
		Message: &controlv1.HubMessage_Certificate{Certificate: &controlv1.CertificateResponse{
			RequestId: request.RequestId, CertificatePem: certificatePEM, CaBundlePem: caPEM,
			NotAfter: timestamppb.New(certificate.NotAfter),
		}},
	}); err != nil {
		return err
	}
	_, err = s.updateSiteStatus(ctx, site.Name, func(current *api.SiteRegistration) error {
		setSiteCondition(&current.Status.Conditions, "IdentityReady", metav1.ConditionTrue,
			"CertificateIssued",
			"A renewed agent mTLS certificate was issued over the authenticated stream")
		return nil
	})
	return err
}

func (s *Server) recordHeartbeat(ctx context.Context, siteName string,
	_ *controlv1.AgentHeartbeat,
) error {
	now := metav1.NewTime(s.now())
	_, err := s.updateSiteStatus(ctx, siteName, func(site *api.SiteRegistration) error {
		site.Status.LastHeartbeatTime = &now
		site.Status.Phase = "Connected"
		setSiteCondition(&site.Status.Conditions, "Connected", metav1.ConditionTrue,
			"HeartbeatReceived", "The site agent heartbeat is current")
		return nil
	})
	return err
}

func (s *Server) recordInventory(ctx context.Context, siteName string,
	update *controlv1.InventoryUpdate,
) error {
	if len(update.InventoryJson) > 1024*1024 {
		return status.Error(codes.ResourceExhausted, "inventory exceeds one MiB")
	}
	var inventory struct {
		StorageClasses        []api.StorageClassInventory        `json:"storageClasses"`
		VolumeSnapshotClasses []api.VolumeSnapshotClassInventory `json:"volumeSnapshotClasses"`
		Issuers               []api.IssuerReference              `json:"issuers"`
	}
	if err := json.Unmarshal(update.InventoryJson, &inventory); err != nil {
		return status.Error(codes.InvalidArgument, "inventory JSON is invalid")
	}
	_, err := s.updateSiteStatus(ctx, siteName, func(site *api.SiteRegistration) error {
		site.Status.DiscoveredStorageClasses = inventory.StorageClasses
		site.Status.DiscoveredVolumeSnapshotClasses = inventory.VolumeSnapshotClasses
		site.Status.DiscoveredIssuers = inventory.Issuers
		return nil
	})
	return err
}

func (s *Server) updateSiteStatus(ctx context.Context, siteName string,
	mutate func(*api.SiteRegistration) error,
) (*api.SiteRegistration, error) {
	var updated api.SiteRegistration
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current api.SiteRegistration
		if err := s.Client.Get(ctx, client.ObjectKey{Name: siteName}, &current); err != nil {
			return err
		}
		if err := mutate(&current); err != nil {
			return err
		}
		if err := s.Client.Status().Update(ctx, &current); err != nil {
			return err
		}
		updated = current
		return nil
	})
	return &updated, err
}

func (s *Server) recordAcknowledgement(ctx context.Context, siteName string,
	ack *controlv1.PlanAcknowledgement,
) error {
	return s.updateInstanceSite(ctx, ack.InstanceUid, siteName, func(site *api.SiteRevisionStatus) {
		if ack.Revision != site.DesiredRevision {
			return
		}
		if ack.Accepted {
			site.AcknowledgedRevision = ack.Revision
			site.Phase = "Applying"
		} else {
			site.Phase = "Rejected"
			setSiteCondition(&site.Conditions, "PlanAccepted", metav1.ConditionFalse,
				"ValidationFailed", strings.Join(ack.ValidationErrors, "; "))
		}
	})
}

func (s *Server) recordProgress(ctx context.Context, siteName string, progress *controlv1.PlanProgress) error {
	hasAddresses := false
	hasConditions := false
	primary, hasTopology := progress.ResourceSummaries["topology/primary"]
	var synchronousStandbys []string
	for key, value := range progress.ResourceSummaries {
		if member, ok := strings.CutPrefix(key, "topology/synchronous/"); ok && value == "healthy" {
			synchronousStandbys = append(synchronousStandbys, member)
			hasTopology = true
		}
	}
	slices.Sort(synchronousStandbys)
	err := s.updateInstanceSite(ctx, progress.InstanceUid, siteName, func(site *api.SiteRevisionStatus) {
		if progress.Revision != site.DesiredRevision {
			return
		}
		site.Phase = progress.Phase
		if site.Addresses == nil {
			site.Addresses = map[string]string{}
		}
		for key, value := range progress.ResourceSummaries {
			if member, ok := strings.CutPrefix(key, "address/"); ok {
				site.Addresses[member] = value
				hasAddresses = true
			}
			if conditionType, ok := strings.CutPrefix(key, "condition/"); ok {
				var condition struct {
					Status  metav1.ConditionStatus `json:"status"`
					Reason  string                 `json:"reason"`
					Message string                 `json:"message"`
				}
				if json.Unmarshal([]byte(value), &condition) == nil {
					setSiteCondition(&site.Conditions, conditionType, condition.Status,
						condition.Reason, condition.Message)
					hasConditions = true
				}
			}
		}
		if hasTopology {
			site.Primary = primary
			site.SynchronousStandbys = synchronousStandbys
			now := metav1.NewTime(s.now())
			site.TopologyObservedAt = &now
		}
	})
	if err != nil || (!hasAddresses && !hasTopology && !hasConditions) {
		return err
	}
	return s.triggerInstanceReconcile(ctx, progress.InstanceUid)
}

func (s *Server) recordResult(ctx context.Context, siteName string, result *controlv1.PlanResult) error {
	if result.OperationUid != "" {
		return s.recordDirectiveResult(ctx, result)
	}
	return s.updateInstanceSite(ctx, result.InstanceUid, siteName, func(site *api.SiteRevisionStatus) {
		if result.AppliedRevision != site.DesiredRevision {
			return
		}
		site.AppliedRevision = result.AppliedRevision
		site.Phase = "Ready"
		for _, condition := range result.Conditions {
			setSiteCondition(&site.Conditions, condition.Type, metav1.ConditionStatus(condition.Status),
				condition.Reason, condition.Message)
			if condition.Type == "Deleted" && condition.Status == string(metav1.ConditionTrue) {
				site.Phase = "Deleted"
			}
		}
	})
}

func (s *Server) recordDirectiveResult(ctx context.Context, result *controlv1.PlanResult) error {
	var configMaps corev1.ConfigMapList
	if err := s.Client.List(ctx, &configMaps, client.HasLabels{
		"multisite-postgres.dev/directive",
	}); err != nil {
		return err
	}
	for i := range configMaps.Items {
		configMap := &configMaps.Items[i]
		if configMap.Data["operationUID"] != result.OperationUid || len(configMap.OwnerReferences) == 0 {
			continue
		}
		return s.recordOwnedDirectiveResult(ctx, configMap, configMap.OwnerReferences[0], result)
	}
	return status.Error(codes.NotFound, "directive operation was not found")
}

func (s *Server) recordOwnedDirectiveResult(ctx context.Context, configMap *corev1.ConfigMap,
	owner metav1.OwnerReference, result *controlv1.PlanResult,
) error {
	switch owner.Kind {
	case "MultiSitePostgres":
		return s.recordInstanceDirectiveResult(ctx, configMap, owner.Name, result)
	case "PostgresDatabase":
		return s.recordDatabaseDirectiveResult(ctx, configMap, owner.Name, result)
	case "PostgresUser":
		return s.recordUserDirectiveResult(ctx, configMap, owner.Name, result)
	case "PostgresRestore":
		return s.recordRestoreDirectiveResult(ctx, configMap, owner.Name, result)
	case "PostgresUpgrade":
		return s.recordUpgradeDirectiveResult(ctx, configMap, owner.Name, result)
	}
	return status.Error(codes.NotFound, "directive operation was not found")
}

func (s *Server) recordInstanceDirectiveResult(ctx context.Context, configMap *corev1.ConfigMap,
	name string, result *controlv1.PlanResult,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var object api.MultiSitePostgres
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: configMap.Namespace, Name: name,
		}, &object); err != nil {
			return err
		}
		succeeded := directiveSucceeded(result.Conditions)
		for _, condition := range result.Conditions {
			setInstanceCondition(&object.Status.Conditions, object.Generation, condition.Type,
				metav1.ConditionStatus(condition.Status), condition.Reason, condition.Message)
		}
		if succeeded {
			updateBackupTimes(&object.Status.LastBackupTime, &object.Status.RecoveryWindowStart, result.Conditions)
			setInstanceCondition(&object.Status.Conditions, object.Generation, "BackupReady",
				metav1.ConditionTrue, "BackupVerified",
				"pgBackRest completed a backup and verified archived WAL metadata")
			if object.Status.RecoveryWindowStart != nil {
				setInstanceCondition(&object.Status.Conditions, object.Generation,
					"RecoveryWindowAvailable", metav1.ConditionTrue, "ContinuousWALVerified",
					"pgBackRest metadata contains a restorable backup and archived WAL range")
			}
		} else {
			setInstanceCondition(&object.Status.Conditions, object.Generation, "BackupReady",
				metav1.ConditionFalse, "BackupFailed", "The scheduled pgBackRest operation failed")
		}
		return s.Client.Status().Update(ctx, &object)
	})
}

func (s *Server) recordDatabaseDirectiveResult(ctx context.Context, configMap *corev1.ConfigMap,
	name string, result *controlv1.PlanResult,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var object api.PostgresDatabase
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: configMap.Namespace, Name: name,
		}, &object); err != nil {
			return err
		}
		applyDirectiveStatus(&object.Status.Phase, &object.Status.Conditions,
			configMap.Data["deleting"] == "true", object.Generation, result.Conditions)
		for _, condition := range result.Conditions {
			if condition.Status != string(metav1.ConditionTrue) {
				continue
			}
			switch condition.Type {
			case "ObservedSize":
				if size, err := strconv.ParseInt(condition.Message, 10, 64); err == nil && size > 0 {
					object.Status.ObservedSize = *resource.NewQuantity(size, resource.BinarySI)
				}
			case "TDEVerified":
				object.Status.TDEVerified = true
			case "OrphanedDeclarations":
				var declarations []string
				if json.Unmarshal([]byte(condition.Message), &declarations) == nil {
					object.Status.OrphanedDeclarations = declarations
				}
			}
		}
		object.Status.ObservedGeneration = object.Generation
		return s.Client.Status().Update(ctx, &object)
	})
}

func (s *Server) recordUserDirectiveResult(ctx context.Context, configMap *corev1.ConfigMap,
	name string, result *controlv1.PlanResult,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var object api.PostgresUser
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: configMap.Namespace, Name: name,
		}, &object); err != nil {
			return err
		}
		applyDirectiveStatus(&object.Status.Phase, &object.Status.Conditions,
			configMap.Data["deleting"] == "true", object.Generation, result.Conditions)
		for _, condition := range result.Conditions {
			if condition.Type == "CredentialVersion" && condition.Status == string(metav1.ConditionTrue) {
				if version, err := strconv.ParseInt(condition.Message, 10, 64); err == nil {
					object.Status.CredentialVersion = version
				}
			}
		}
		object.Status.ObservedGeneration = object.Generation
		return s.Client.Status().Update(ctx, &object)
	})
}

func (s *Server) recordRestoreDirectiveResult(ctx context.Context, configMap *corev1.ConfigMap,
	name string, result *controlv1.PlanResult,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var object api.PostgresRestore
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: configMap.Namespace, Name: name,
		}, &object); err != nil {
			return err
		}
		applyDirectiveStatus(&object.Status.Phase, &object.Status.Conditions,
			false, object.Generation, result.Conditions)
		object.Status.ObservedGeneration = object.Generation
		return s.Client.Status().Update(ctx, &object)
	})
}

func (s *Server) recordUpgradeDirectiveResult(ctx context.Context, configMap *corev1.ConfigMap,
	name string, result *controlv1.PlanResult,
) error {
	if configMap.Data["type"] == "Backup" {
		return s.recordUpgradeBackupResult(ctx, configMap, name, result)
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var object api.PostgresUpgrade
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: configMap.Namespace, Name: name,
		}, &object); err != nil {
			return err
		}
		applyDirectiveStatus(&object.Status.Phase, &object.Status.Conditions,
			false, object.Generation, result.Conditions)
		object.Status.ObservedGeneration = object.Generation
		return s.Client.Status().Update(ctx, &object)
	})
}

func (s *Server) recordUpgradeBackupResult(ctx context.Context, configMap *corev1.ConfigMap,
	name string, result *controlv1.PlanResult,
) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var object api.PostgresUpgrade
		if err := s.Client.Get(ctx, client.ObjectKey{Namespace: configMap.Namespace, Name: name}, &object); err != nil {
			return err
		}
		conditionType := "FreshBackupReady"
		successMessage := "pgBackRest completed a fresh full backup and verified archived WAL"
		if configMap.Data["upgradeBackupPhase"] == "post-upgrade" {
			conditionType = "PostUpgradeBackupReady"
			successMessage = "pgBackRest completed the post-upgrade full backup and verified archived WAL"
		}
		if directiveSucceeded(result.Conditions) {
			setInstanceCondition(&object.Status.Conditions, object.Generation,
				conditionType, metav1.ConditionTrue, "BackupVerified", successMessage)
		} else {
			setInstanceCondition(&object.Status.Conditions, object.Generation,
				conditionType, metav1.ConditionFalse, "BackupFailed",
				"The required full backup or WAL verification failed")
		}
		return s.Client.Status().Update(ctx, &object)
	}); err != nil || !directiveSucceeded(result.Conditions) {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var instance api.MultiSitePostgres
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: configMap.Namespace, Name: configMap.Data["instanceRef"],
		}, &instance); err != nil {
			return err
		}
		updateBackupTimes(&instance.Status.LastBackupTime, &instance.Status.RecoveryWindowStart,
			result.Conditions)
		setInstanceCondition(&instance.Status.Conditions, instance.Generation,
			"BackupReady", metav1.ConditionTrue, "BackupVerified",
			"pgBackRest completed a backup and verified archived WAL metadata")
		return s.Client.Status().Update(ctx, &instance)
	})
}

func updateBackupTimes(lastBackup, recoveryWindow **metav1.Time, conditions []*controlv1.Condition) {
	for _, condition := range conditions {
		var target **metav1.Time
		switch condition.Type {
		case "BackupCompletedAt":
			target = lastBackup
		case "RecoveryWindowStart":
			target = recoveryWindow
		default:
			continue
		}
		if parsed, err := time.Parse(time.RFC3339, condition.Message); err == nil {
			value := metav1.NewTime(parsed)
			*target = &value
		}
	}
}

func directiveSucceeded(reported []*controlv1.Condition) bool {
	found := false
	for _, condition := range reported {
		if condition.Type == "Succeeded" {
			found = true
			if condition.Status != string(metav1.ConditionTrue) {
				return false
			}
		}
	}
	return found
}

func applyDirectiveStatus(phase *string, conditions *[]metav1.Condition, deleting bool, generation int64,
	reported []*controlv1.Condition,
) {
	succeeded := directiveSucceeded(reported)
	for _, condition := range reported {
		setSiteCondition(conditions, condition.Type, metav1.ConditionStatus(condition.Status),
			condition.Reason, condition.Message)
		if observed := meta.FindStatusCondition(*conditions, condition.Type); observed != nil {
			observed.ObservedGeneration = generation
		}
	}
	if !succeeded {
		*phase = "Failed"
		return
	}
	if deleting {
		*phase = "Deleted"
	} else {
		*phase = "Ready"
	}
}

func (s *Server) updateInstanceSite(ctx context.Context, instanceUID, siteName string,
	update func(*api.SiteRevisionStatus),
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var instances api.MultiSitePostgresList
		if err := s.Client.List(ctx, &instances); err != nil {
			return err
		}
		for i := range instances.Items {
			instance := &instances.Items[i]
			if string(instance.UID) != instanceUID {
				continue
			}
			for j := range instance.Status.Sites {
				if instance.Status.Sites[j].Name == siteName {
					update(&instance.Status.Sites[j])
					for k := range instance.Status.Sites[j].Conditions {
						instance.Status.Sites[j].Conditions[k].ObservedGeneration = instance.Generation
					}
					aggregateInstanceConditions(instance)
					if instance.DeletionTimestamp.IsZero() &&
						allApplied(instance.Status.Sites, instance.Status.ActiveRevision) {
						instance.Status.Phase = "Ready"
						setSiteCondition(&instance.Status.Conditions, "Ready", metav1.ConditionTrue,
							"AllSitesReady", "All sites applied the active revision")
					}
					return s.Client.Status().Update(ctx, instance)
				}
			}
			return status.Error(codes.NotFound, "site is not part of the instance")
		}
		return status.Error(codes.NotFound, "instance UID was not found")
	})
}

func aggregateInstanceConditions(instance *api.MultiSitePostgres) {
	for _, conditionType := range []string{
		"LoadBalancersAllocated", "CertificatesReady", "EtcdTLSReady", "EtcdQuorate", "PatroniReady",
		"TDEVerified",
	} {
		if conditionType == "TDEVerified" && !instance.Spec.TDE.Enabled {
			continue
		}
		ready := true
		applicable := 0
		for _, site := range instance.Status.Sites {
			if (conditionType == "PatroniReady" || conditionType == "TDEVerified") &&
				siteRole(instance, site.Name) == api.SiteRoleWitness {
				continue
			}
			applicable++
			condition := meta.FindStatusCondition(site.Conditions, conditionType)
			if condition == nil || condition.Status != metav1.ConditionTrue {
				ready = false
			}
		}
		statusValue := metav1.ConditionFalse
		reason := "AwaitingSites"
		message := "Waiting for all applicable sites to report " + conditionType
		if ready && applicable > 0 {
			statusValue = metav1.ConditionTrue
			reason = "AllSitesReady"
			message = "All applicable sites report " + conditionType
		}
		meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type: conditionType, Status: statusValue, ObservedGeneration: instance.Generation,
			Reason: reason, Message: message,
		})
	}
	aggregateCommonTrust(instance, "EtcdTLSReady", false, "etcd")
	aggregateBackupTLS(instance)
}

func aggregateBackupTLS(instance *api.MultiSitePostgres) {
	if instance.Spec.Backup == nil {
		return
	}
	aggregateCommonTrust(instance, "BackupTLSReady", true, "pgBackRest")
}

func aggregateCommonTrust(instance *api.MultiSitePostgres, conditionType string,
	dataSitesOnly bool, component string,
) {
	var fingerprint string
	applicable := 0
	for _, site := range instance.Status.Sites {
		if dataSitesOnly && siteRole(instance, site.Name) == api.SiteRoleWitness {
			continue
		}
		applicable++
		condition := meta.FindStatusCondition(site.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionTrue {
			setInstanceCondition(&instance.Status.Conditions, instance.Generation, conditionType,
				metav1.ConditionFalse, "AwaitingSites",
				"Waiting for all applicable sites to report their "+component+" trust bundle")
			return
		}
		if fingerprint == "" {
			fingerprint = condition.Message
		} else if condition.Message != fingerprint {
			setInstanceCondition(&instance.Status.Conditions, instance.Generation, conditionType,
				metav1.ConditionFalse, "TrustBundleMismatch",
				component+" issuers do not publish the same CA bundle across sites")
			return
		}
	}
	if applicable == 0 {
		return
	}
	setInstanceCondition(&instance.Status.Conditions, instance.Generation, conditionType,
		metav1.ConditionTrue, "CommonTrustBundle",
		"All applicable sites use the same "+component+" CA bundle")
}

func siteRole(instance *api.MultiSitePostgres, siteName string) api.SiteRole {
	for _, site := range instance.Spec.Sites {
		if site.Name == siteName {
			return site.Role
		}
	}
	return ""
}

func (s *Server) triggerInstanceReconcile(ctx context.Context, instanceUID string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var instances api.MultiSitePostgresList
		if err := s.Client.List(ctx, &instances); err != nil {
			return err
		}
		for i := range instances.Items {
			instance := &instances.Items[i]
			if string(instance.UID) != instanceUID {
				continue
			}
			base := instance.DeepCopy()
			if instance.Annotations == nil {
				instance.Annotations = map[string]string{}
			}
			instance.Annotations["multisite-postgres.dev/address-observation"] =
				s.now().UTC().Format(time.RFC3339Nano)
			return s.Client.Patch(ctx, instance, client.MergeFrom(base))
		}
		return status.Error(codes.NotFound, "instance UID was not found")
	})
}

func validatePeerIdentity(ctx context.Context, registrationUID string) (*x509.Certificate, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, errors.New("mTLS peer information is missing")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, errors.New("mTLS client certificate is missing")
	}
	certificate := tlsInfo.State.PeerCertificates[0]
	if certificate.Subject.CommonName == registrationUID || certificateHasURI(certificate, registrationUID) {
		return certificate, nil
	}
	return nil, errors.New("mTLS certificate identity does not match registration UID")
}

func certificateHasURI(certificate *x509.Certificate, identity string) bool {
	for _, uri := range certificate.URIs {
		if uri.String() == "spiffe://multisite-postgres.dev/site/"+identity {
			return true
		}
	}
	return false
}

func allApplied(sites []api.SiteRevisionStatus, revision int64) bool {
	for _, site := range sites {
		if site.AppliedRevision != revision {
			return false
		}
	}
	return len(sites) > 0
}

func setSiteCondition(conditions *[]metav1.Condition, conditionType string,
	conditionStatus metav1.ConditionStatus, reason, message string,
) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: conditionType, Status: conditionStatus, Reason: reason, Message: message,
	})
}

func setInstanceCondition(conditions *[]metav1.Condition, generation int64, conditionType string,
	conditionStatus metav1.ConditionStatus, reason, message string,
) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: conditionType, Status: conditionStatus, Reason: reason, Message: message,
		ObservedGeneration: generation, LastTransitionTime: metav1.Now(),
	})
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
