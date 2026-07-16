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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"github.com/sindef/mspsql/internal/agent"
	"github.com/sindef/mspsql/internal/plan"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AgentClient struct {
	Target       string
	DialOptions  []grpc.DialOption
	Hello        *controlv1.AgentHello
	Cache        *agent.Cache
	Reconciler   *agent.Reconciler
	sendMu       sync.Mutex
	activeMu     sync.Mutex
	active       map[string]int64
	heartbeatNow func() time.Time
}

func (c *AgentClient) Run(ctx context.Context) error {
	connection, err := grpc.NewClient(c.Target, c.DialOptions...)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	stream, err := controlv1.NewAgentControlClient(connection).Connect(ctx)
	if err != nil {
		return err
	}
	if err := c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Hello{Hello: c.Hello},
	}); err != nil {
		return err
	}
	if c.active == nil {
		c.active = map[string]int64{}
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErr := make(chan error, 1)
	go func() { heartbeatErr <- c.heartbeats(heartbeatCtx, stream) }()

	for {
		message, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if message.GetPlan() == nil {
			continue
		}
		if err := c.applyPlan(ctx, stream, message.GetPlan()); err != nil {
			return err
		}
		select {
		case err := <-heartbeatErr:
			return err
		default:
		}
	}
}

func (c *AgentClient) applyPlan(ctx context.Context, stream controlv1.AgentControl_ConnectClient,
	message *controlv1.DesiredSitePlan,
) error {
	var envelope plan.Envelope
	if err := json.Unmarshal(message.EnvelopeJson, &envelope); err != nil {
		return c.reject(stream, message, err)
	}
	previous, err := c.Cache.Load(ctx, message.InstanceUid)
	if err != nil && !apierrors.IsNotFound(err) {
		return c.reject(stream, message, err)
	}
	desired, err := c.Cache.Store(ctx, envelope, message.InstanceUid)
	if err != nil {
		return c.reject(stream, message, err)
	}
	if err := c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Acknowledgement{Acknowledgement: &controlv1.PlanAcknowledgement{
			InstanceUid: message.InstanceUid, Revision: message.Revision, Accepted: true,
		}},
	}); err != nil {
		return err
	}
	if err := c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Progress{Progress: &controlv1.PlanProgress{
			InstanceUid: message.InstanceUid, Revision: message.Revision, Phase: "Applying",
		}},
	}); err != nil {
		return err
	}
	result, err := c.Reconciler.Apply(ctx, desired, previous, true)
	if err != nil {
		return err
	}
	summaries := make(map[string]string, len(result.Addresses))
	for member, address := range result.Addresses {
		summaries["address/"+member] = address
	}
	if err := c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Progress{Progress: &controlv1.PlanProgress{
			InstanceUid: message.InstanceUid, Revision: message.Revision,
			Phase: result.Phase, ResourceSummaries: summaries,
		}},
	}); err != nil {
		return err
	}
	if result.Phase != "Ready" {
		return nil
	}
	conditions := make([]*controlv1.Condition, 0, len(result.Conditions))
	for _, condition := range result.Conditions {
		conditions = append(conditions, &controlv1.Condition{
			Type: condition.Type, Status: string(condition.Status), Reason: condition.Reason,
			Message: condition.Message, LastTransitionTime: timestamppb.New(condition.LastTransitionTime.Time),
		})
	}
	if err := c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Result{Result: &controlv1.PlanResult{
			InstanceUid: message.InstanceUid, AppliedRevision: message.Revision, Conditions: conditions,
		}},
	}); err != nil {
		return err
	}
	c.activeMu.Lock()
	c.active[message.InstanceUid] = message.Revision
	c.activeMu.Unlock()
	return nil
}

func (c *AgentClient) reject(stream controlv1.AgentControl_ConnectClient,
	message *controlv1.DesiredSitePlan, validationErr error,
) error {
	if err := c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Acknowledgement{Acknowledgement: &controlv1.PlanAcknowledgement{
			InstanceUid: message.InstanceUid, Revision: message.Revision, Accepted: false,
			ValidationErrors: []string{validationErr.Error()},
		}},
	}); err != nil {
		return err
	}
	return fmt.Errorf("reject plan revision %d: %w", message.Revision, validationErr)
}

func (c *AgentClient) heartbeats(ctx context.Context, stream controlv1.AgentControl_ConnectClient) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		c.activeMu.Lock()
		revisions := make(map[string]int64, len(c.active))
		maps.Copy(revisions, c.active)
		c.activeMu.Unlock()
		if err := c.send(stream, &controlv1.AgentMessage{
			Message: &controlv1.AgentMessage_Heartbeat{Heartbeat: &controlv1.AgentHeartbeat{
				ActiveRevisions: revisions, SentAt: timestamppb.New(c.now()),
			}},
		}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *AgentClient) send(stream controlv1.AgentControl_ConnectClient, message *controlv1.AgentMessage) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return stream.Send(message)
}

func (c *AgentClient) now() time.Time {
	if c.heartbeatNow != nil {
		return c.heartbeatNow()
	}
	return time.Now()
}
