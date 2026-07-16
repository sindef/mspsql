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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"github.com/sindef/mspsql/internal/plan"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Server struct {
	controlv1.UnimplementedAgentControlServer
	Client client.Client
	Now    func() time.Time
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
	if err := validatePeerIdentity(stream.Context(), hello.RegistrationUid); err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	site, err := s.bindSite(stream.Context(), hello)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	errs := make(chan error, 2)
	go func() { errs <- s.sendPlans(ctx, stream, string(site.UID)) }()
	go func() { errs <- s.receive(ctx, stream, site.Name) }()
	err = <-errs
	cancel()
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *Server) bindSite(ctx context.Context, hello *controlv1.AgentHello) (*api.SiteRegistration, error) {
	var sites api.SiteRegistrationList
	if err := s.Client.List(ctx, &sites); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for i := range sites.Items {
		site := &sites.Items[i]
		if string(site.UID) != hello.RegistrationUid {
			continue
		}
		if site.Status.ClusterUID != "" && site.Status.ClusterUID != hello.ClusterUid {
			return nil, status.Error(codes.PermissionDenied,
				"registration is permanently bound to another Kubernetes cluster UID")
		}
		now := metav1.NewTime(s.now())
		site.Status.ClusterUID = hello.ClusterUid
		site.Status.Phase = "Connected"
		site.Status.AgentVersion = hello.AgentVersion
		site.Status.Capabilities = append([]string(nil), hello.Capabilities...)
		site.Status.LastHeartbeatTime = &now
		if err := s.Client.Status().Update(ctx, site); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return site, nil
	}
	return nil, status.Error(codes.NotFound, "site registration UID was not found")
}

func (s *Server) sendPlans(ctx context.Context, stream controlv1.AgentControl_ConnectServer,
	siteUID string,
) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	sent := map[string]int64{}
	for {
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
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) receive(ctx context.Context, stream controlv1.AgentControl_ConnectServer,
	siteName string,
) error {
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case message.GetHeartbeat() != nil:
			if err := s.recordHeartbeat(ctx, siteName, message.GetHeartbeat()); err != nil {
				return err
			}
		case message.GetAcknowledgement() != nil:
			if err := s.recordAcknowledgement(ctx, siteName, message.GetAcknowledgement()); err != nil {
				return err
			}
		case message.GetProgress() != nil:
			if err := s.recordProgress(ctx, siteName, message.GetProgress()); err != nil {
				return err
			}
		case message.GetResult() != nil:
			if err := s.recordResult(ctx, siteName, message.GetResult()); err != nil {
				return err
			}
		case message.GetInventory() != nil:
			if err := s.recordInventory(ctx, siteName, message.GetInventory()); err != nil {
				return err
			}
		}
	}
}

func (s *Server) recordHeartbeat(ctx context.Context, siteName string,
	heartbeat *controlv1.AgentHeartbeat,
) error {
	var site api.SiteRegistration
	if err := s.Client.Get(ctx, client.ObjectKey{Name: siteName}, &site); err != nil {
		return err
	}
	heartbeatTime := s.now()
	if heartbeat.SentAt != nil && heartbeat.SentAt.IsValid() {
		heartbeatTime = heartbeat.SentAt.AsTime()
	}
	now := metav1.NewTime(heartbeatTime)
	site.Status.LastHeartbeatTime = &now
	site.Status.Phase = "Connected"
	return s.Client.Status().Update(ctx, &site)
}

func (s *Server) recordInventory(ctx context.Context, siteName string,
	update *controlv1.InventoryUpdate,
) error {
	if len(update.InventoryJson) > 1024*1024 {
		return status.Error(codes.ResourceExhausted, "inventory exceeds one MiB")
	}
	var inventory struct {
		StorageClasses []api.StorageClassInventory `json:"storageClasses"`
		Issuers        []api.IssuerReference       `json:"issuers"`
	}
	if err := json.Unmarshal(update.InventoryJson, &inventory); err != nil {
		return status.Error(codes.InvalidArgument, "inventory JSON is invalid")
	}
	var site api.SiteRegistration
	if err := s.Client.Get(ctx, client.ObjectKey{Name: siteName}, &site); err != nil {
		return err
	}
	site.Status.DiscoveredStorageClasses = inventory.StorageClasses
	site.Status.DiscoveredIssuers = inventory.Issuers
	return s.Client.Status().Update(ctx, &site)
}

func (s *Server) recordAcknowledgement(ctx context.Context, siteName string,
	ack *controlv1.PlanAcknowledgement,
) error {
	return s.updateInstanceSite(ctx, ack.InstanceUid, siteName, func(site *api.SiteRevisionStatus) {
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
	err := s.updateInstanceSite(ctx, progress.InstanceUid, siteName, func(site *api.SiteRevisionStatus) {
		site.Phase = progress.Phase
		if site.Addresses == nil {
			site.Addresses = map[string]string{}
		}
		for key, value := range progress.ResourceSummaries {
			if member, ok := strings.CutPrefix(key, "address/"); ok {
				site.Addresses[member] = value
				hasAddresses = true
			}
		}
	})
	if err != nil || !hasAddresses {
		return err
	}
	return s.triggerInstanceReconcile(ctx, progress.InstanceUid)
}

func (s *Server) recordResult(ctx context.Context, siteName string, result *controlv1.PlanResult) error {
	return s.updateInstanceSite(ctx, result.InstanceUid, siteName, func(site *api.SiteRevisionStatus) {
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

func (s *Server) updateInstanceSite(ctx context.Context, instanceUID, siteName string,
	update func(*api.SiteRevisionStatus),
) error {
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
}

func aggregateInstanceConditions(instance *api.MultiSitePostgres) {
	for _, conditionType := range []string{
		"LoadBalancersAllocated", "CertificatesReady", "EtcdQuorate", "PatroniReady",
	} {
		ready := true
		applicable := 0
		for _, site := range instance.Status.Sites {
			if conditionType == "PatroniReady" && siteRole(instance, site.Name) == api.SiteRoleWitness {
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
}

func validatePeerIdentity(ctx context.Context, registrationUID string) error {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return errors.New("mTLS peer information is missing")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return errors.New("mTLS client certificate is missing")
	}
	certificate := tlsInfo.State.PeerCertificates[0]
	if certificate.Subject.CommonName == registrationUID || certificateHasURI(certificate, registrationUID) {
		return nil
	}
	return errors.New("mTLS certificate identity does not match registration UID")
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
	for i := range *conditions {
		if (*conditions)[i].Type == conditionType {
			(*conditions)[i].Status = conditionStatus
			(*conditions)[i].Reason = reason
			(*conditions)[i].Message = message
			(*conditions)[i].LastTransitionTime = metav1.NewTime(time.Now())
			return
		}
	}
	*conditions = append(*conditions, metav1.Condition{
		Type: conditionType, Status: conditionStatus, Reason: reason, Message: message,
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
