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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	controlv1 "github.com/sindef/mspsql/gen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type RunnableServer struct {
	Address     string
	Certificate string
	PrivateKey  string
	ClientCA    string
	Service     *Server
}

func (r *RunnableServer) Start(ctx context.Context) error {
	certificate, err := tls.LoadX509KeyPair(r.Certificate, r.PrivateKey)
	if err != nil {
		return fmt.Errorf("load gRPC server identity: %w", err)
	}
	caPEM, err := os.ReadFile(r.ClientCA)
	if err != nil {
		return fmt.Errorf("read gRPC client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("gRPC client CA contains no certificates")
	}
	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs,
		})),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 2 * time.Minute, Timeout: 20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: time.Minute, PermitWithoutStream: false,
		}),
	)
	controlv1.RegisterAgentControlServer(server, r.Service)
	listener, err := net.Listen("tcp", r.Address)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	return server.Serve(listener)
}

func (r *RunnableServer) NeedLeaderElection() bool {
	return false
}
