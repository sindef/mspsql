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
	"io"
	"maps"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"github.com/sindef/mspsql/internal/agent"
	"github.com/sindef/mspsql/internal/directive"
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
	Directives   DirectiveExecutor
	Inventory    func(context.Context) ([]byte, error)
	sendMu       sync.Mutex
	activeMu     sync.Mutex
	active       map[string]int64
	heartbeatNow func() time.Time
}

type DirectiveExecutor interface {
	Execute(context.Context, directive.Payload) ([]metav1.Condition, error)
}

type receivedHubMessage struct {
	message *controlv1.HubMessage
	err     error
}

type planWorkerResult struct {
	instanceUID string
	revision    int64
	err         error
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
	if err := c.sendInventory(ctx, stream); err != nil {
		return err
	}
	if c.active == nil {
		c.active = map[string]int64{}
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErr := make(chan error, 1)
	go func() { heartbeatErr <- c.heartbeats(heartbeatCtx, stream) }()

	received := make(chan receivedHubMessage, 1)
	go func() {
		for {
			message, receiveErr := stream.Recv()
			select {
			case received <- receivedHubMessage{message: message, err: receiveErr}:
			case <-ctx.Done():
				return
			}
			if receiveErr != nil {
				return
			}
		}
	}()
	workers := map[string]struct {
		revision int64
		cancel   context.CancelFunc
	}{}
	workerResults := make(chan planWorkerResult)
	defer func() {
		for _, worker := range workers {
			worker.cancel()
		}
	}()

	for {
		select {
		case receive := <-received:
			if receive.err != nil {
				if errors.Is(receive.err, io.EOF) {
					return nil
				}
				return receive.err
			}
			message := receive.message
			if message.GetDirective() != nil {
				if err := c.applyDirective(ctx, stream, message.GetDirective()); err != nil {
					return err
				}
				continue
			}
			if message.GetPlan() == nil {
				continue
			}
			desired := message.GetPlan()
			if worker, exists := workers[desired.InstanceUid]; exists {
				if worker.revision >= desired.Revision {
					continue
				}
				worker.cancel()
			}
			workerCtx, cancelWorker := context.WithCancel(ctx)
			workers[desired.InstanceUid] = struct {
				revision int64
				cancel   context.CancelFunc
			}{revision: desired.Revision, cancel: cancelWorker}
			go func(desiredPlan *controlv1.DesiredSitePlan) {
				result := planWorkerResult{
					instanceUID: desiredPlan.InstanceUid,
					revision:    desiredPlan.Revision,
					err:         c.applyPlan(workerCtx, stream, desiredPlan),
				}
				select {
				case workerResults <- result:
				case <-ctx.Done():
				}
			}(desired)
		case result := <-workerResults:
			worker, exists := workers[result.instanceUID]
			if exists && worker.revision == result.revision {
				delete(workers, result.instanceUID)
			}
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				return result.err
			}
		case err := <-heartbeatErr:
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *AgentClient) applyDirective(ctx context.Context, stream controlv1.AgentControl_ConnectClient,
	message *controlv1.OperationDirective,
) error {
	var envelope plan.Envelope
	if err := json.Unmarshal(message.DirectiveJson, &envelope); err != nil {
		return err
	}
	payload, err := directive.Verify(c.Cache.PublicKey, envelope, c.Reconciler.SiteUID,
		message.InstanceUid, message.OperationUid)
	if err != nil {
		return err
	}
	if c.Directives == nil {
		return errors.New("site agent has no directive executor")
	}
	if err := c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Progress{Progress: &controlv1.PlanProgress{
			OperationUid: message.OperationUid, InstanceUid: message.InstanceUid, Phase: "Running",
		}},
	}); err != nil {
		return err
	}
	conditions, executeErr := c.Directives.Execute(ctx, payload)
	if executeErr != nil {
		conditions = append(conditions, metav1.Condition{
			Type: "Succeeded", Status: metav1.ConditionFalse,
			Reason: "ExecutionFailed", Message: executeErr.Error(), LastTransitionTime: metav1.Now(),
		})
	}
	protoConditions := make([]*controlv1.Condition, 0, len(conditions))
	for _, condition := range conditions {
		protoConditions = append(protoConditions, &controlv1.Condition{
			Type: condition.Type, Status: string(condition.Status), Reason: condition.Reason,
			Message: condition.Message, LastTransitionTime: timestamppb.New(condition.LastTransitionTime.Time),
		})
	}
	return c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Result{Result: &controlv1.PlanResult{
			OperationUid: message.OperationUid, InstanceUid: message.InstanceUid,
			Conditions: protoConditions,
		}},
	})
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
	backoff := time.Second
	resultReported := false
	for {
		result, applyErr := c.Reconciler.Apply(ctx, desired, previous, true)
		summaries := make(map[string]string, len(result.Addresses)+1)
		for member, address := range result.Addresses {
			summaries["address/"+member] = address
		}
		if result.Primary != "" {
			summaries["topology/primary"] = result.Primary
		}
		for _, member := range result.SynchronousStandbys {
			summaries["topology/synchronous/"+member] = "healthy"
		}
		if applyErr != nil {
			summaries["error"] = applyErr.Error()
			result.Phase = "Retrying"
		}
		if err := c.send(stream, &controlv1.AgentMessage{
			Message: &controlv1.AgentMessage_Progress{Progress: &controlv1.PlanProgress{
				InstanceUid: message.InstanceUid, Revision: message.Revision,
				Phase: result.Phase, ResourceSummaries: summaries,
			}},
		}); err != nil {
			return err
		}
		if applyErr == nil && (result.Phase == "Ready" || result.Phase == "Deleted") &&
			!resultReported {
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
			resultReported = true
			if result.Phase == "Deleted" {
				return nil
			}
		}
		delay := 5 * time.Second
		if applyErr == nil && result.Phase == "Ready" {
			delay = 30 * time.Second
		} else if applyErr != nil {
			delay = backoff
			if backoff < time.Minute {
				backoff *= 2
			}
		} else {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
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
	return nil
}

func (c *AgentClient) heartbeats(ctx context.Context, stream controlv1.AgentControl_ConnectClient) error {
	heartbeatTicker := time.NewTicker(time.Minute)
	inventoryTicker := time.NewTicker(10 * time.Minute)
	defer heartbeatTicker.Stop()
	defer inventoryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
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
		case <-inventoryTicker.C:
			if err := c.sendInventory(ctx, stream); err != nil {
				return err
			}
		}
	}
}

func (c *AgentClient) sendInventory(ctx context.Context, stream controlv1.AgentControl_ConnectClient) error {
	if c.Inventory == nil {
		return nil
	}
	inventory, err := c.Inventory(ctx)
	if err != nil {
		return err
	}
	return c.send(stream, &controlv1.AgentMessage{
		Message: &controlv1.AgentMessage_Inventory{Inventory: &controlv1.InventoryUpdate{
			InventoryJson: inventory,
		}},
	})
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
