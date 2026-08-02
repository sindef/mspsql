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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"github.com/sindef/mspsql/internal/agent"
	"github.com/sindef/mspsql/internal/directive"
)

func TestApplyDirectiveReportsCancellation(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := directive.Sign(privateKey, directive.Payload{
		SiteUID: "site", InstanceUID: "instance", ObjectUID: "backup",
		OperationUID: "operation", Type: "Backup", GeneratedAt: time.Now(),
		Spec: json.RawMessage(`{"type":"full"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	rawEnvelope, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	executorEntered := make(chan struct{})
	client := AgentClient{
		Cache:      &agent.Cache{PublicKey: publicKey},
		Reconciler: &agent.Reconciler{SiteUID: "site"},
		Directives: blockingDirectiveExecutor(func(ctx context.Context, payload directive.Payload) ([]metav1.Condition, error) {
			if payload.OperationUID != "operation" {
				t.Fatalf("payload operation UID = %q", payload.OperationUID)
			}
			close(executorEntered)
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	}
	stream := &fakeControlStream{ctx: ctx}
	done := make(chan error, 1)
	go func() {
		done <- client.applyDirective(ctx, stream, &controlv1.OperationDirective{
			OperationUid: "operation", InstanceUid: "instance", Type: "Backup", DirectiveJson: rawEnvelope,
		})
	}()
	select {
	case <-executorEntered:
	case <-time.After(time.Second):
		t.Fatal("directive executor was not started")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("applyDirective returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("applyDirective did not finish after cancellation")
	}
	if len(stream.sent) != 2 {
		t.Fatalf("sent messages = %d", len(stream.sent))
	}
	progress := stream.sent[0].GetProgress()
	if progress == nil || progress.OperationUid != "operation" || progress.Phase != "Running" {
		t.Fatalf("progress = %#v", progress)
	}
	result := stream.sent[1].GetResult()
	if result == nil || result.OperationUid != "operation" || len(result.Conditions) != 1 {
		t.Fatalf("result = %#v", result)
	}
	condition := result.Conditions[0]
	if condition.Type != "Succeeded" || condition.Status != string(metav1.ConditionFalse) ||
		condition.Reason != "ExecutionFailed" {
		t.Fatalf("cancellation condition = %#v", condition)
	}
}

type blockingDirectiveExecutor func(context.Context, directive.Payload) ([]metav1.Condition, error)

func (e blockingDirectiveExecutor) Execute(ctx context.Context,
	payload directive.Payload,
) ([]metav1.Condition, error) {
	return e(ctx, payload)
}

type fakeControlStream struct {
	ctx  context.Context
	sent []*controlv1.AgentMessage
}

func (s *fakeControlStream) Send(message *controlv1.AgentMessage) error {
	s.sent = append(s.sent, message)
	return nil
}

func (s *fakeControlStream) Recv() (*controlv1.HubMessage, error) { return nil, io.EOF }
func (s *fakeControlStream) Header() (metadata.MD, error)         { return nil, nil }
func (s *fakeControlStream) Trailer() metadata.MD                 { return nil }
func (s *fakeControlStream) CloseSend() error                     { return nil }
func (s *fakeControlStream) Context() context.Context             { return s.ctx }
func (s *fakeControlStream) SendMsg(any) error                    { return nil }
func (s *fakeControlStream) RecvMsg(any) error                    { return io.EOF }

var _ controlv1.AgentControl_ConnectClient = (*fakeControlStream)(nil)
