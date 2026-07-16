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
	"testing"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

func TestAllocateInjectsTunDevice(t *testing.T) {
	plugin := &tunDevicePlugin{devices: makeDevices(2)}
	response, err := plugin.Allocate(context.Background(), &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: []string{"tun-1"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	device := response.ContainerResponses[0].Devices[0]
	if device.HostPath != tunPath || device.ContainerPath != tunPath || device.Permissions != "mrw" {
		t.Fatalf("device allocation = %#v", device)
	}
	if _, err := plugin.Allocate(context.Background(), &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: []string{"missing"}}},
	}); err == nil {
		t.Fatal("unknown device allocation was accepted")
	}
}
