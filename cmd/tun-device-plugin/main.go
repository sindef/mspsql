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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	resourceName = "multisite-postgres.dev/tun"
	socketName   = "mspsql-tun.sock"
	tunPath      = "/dev/net/tun"
)

type tunDevicePlugin struct {
	pluginapi.UnimplementedDevicePluginServer
	devices []*pluginapi.Device
}

func main() {
	var capacity int
	flag.IntVar(&capacity, "capacity", 64, "Virtual TUN allocations advertised per node.")
	flag.Parse()
	if capacity < 1 {
		log.Fatal("capacity must be positive")
	}
	if _, err := os.Stat(tunPath); err != nil {
		log.Fatalf("%s is unavailable: %v", tunPath, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, capacity); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, capacity int) error {
	socketPath := filepath.Join(pluginapi.DevicePluginPath, socketName)
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}()
	plugin := &tunDevicePlugin{devices: makeDevices(capacity)}
	server := grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(server, plugin)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	if err := register(ctx, socketName); err != nil {
		server.Stop()
		return err
	}
	select {
	case <-ctx.Done():
		server.GracefulStop()
		return nil
	case err := <-serveErrors:
		return err
	}
}

func register(ctx context.Context, endpoint string) error {
	dialer := func(context.Context, string) (net.Conn, error) {
		return net.DialTimeout("unix", pluginapi.KubeletSocket, 5*time.Second)
	}
	connection, err := grpc.NewClient("passthrough:///kubelet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer))
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	registrationCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = pluginapi.NewRegistrationClient(connection).Register(registrationCtx,
		&pluginapi.RegisterRequest{
			Version: pluginapi.Version, Endpoint: endpoint, ResourceName: resourceName,
		})
	return err
}

func makeDevices(capacity int) []*pluginapi.Device {
	devices := make([]*pluginapi.Device, capacity)
	for i := range capacity {
		devices[i] = &pluginapi.Device{ID: fmt.Sprintf("tun-%d", i), Health: pluginapi.Healthy}
	}
	return devices
}

func (p *tunDevicePlugin) GetDevicePluginOptions(context.Context,
	*pluginapi.Empty,
) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (p *tunDevicePlugin) ListAndWatch(_ *pluginapi.Empty,
	stream grpc.ServerStreamingServer[pluginapi.ListAndWatchResponse],
) error {
	if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devices}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (p *tunDevicePlugin) Allocate(_ context.Context,
	request *pluginapi.AllocateRequest,
) (*pluginapi.AllocateResponse, error) {
	known := make(map[string]struct{}, len(p.devices))
	for _, device := range p.devices {
		known[device.ID] = struct{}{}
	}
	response := &pluginapi.AllocateResponse{
		ContainerResponses: make([]*pluginapi.ContainerAllocateResponse, 0,
			len(request.ContainerRequests)),
	}
	for _, containerRequest := range request.ContainerRequests {
		for _, id := range containerRequest.DevicesIds {
			if _, found := known[id]; !found {
				return nil, fmt.Errorf("unknown TUN allocation %q", id)
			}
		}
		response.ContainerResponses = append(response.ContainerResponses,
			&pluginapi.ContainerAllocateResponse{Devices: []*pluginapi.DeviceSpec{{
				HostPath: tunPath, ContainerPath: tunPath, Permissions: "mrw",
			}}})
	}
	return response, nil
}
