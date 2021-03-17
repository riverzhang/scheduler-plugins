/*
Copyright 2020 The Kubernetes Authors.

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
package device

import (
	"fmt"

	"sigs.k8s.io/scheduler-plugins/pkg/gpu/util"
)

type DeviceInfo struct {
	id          int
	totalMemory uint
	usedMemory  uint
	usedCore    uint
}

func newDeviceInfo(id int, totalMemory uint) *DeviceInfo {
	return &DeviceInfo{
		id:          id,
		totalMemory: totalMemory,
	}
}

// GetID returns the idx of this device
func (dev *DeviceInfo) GetID() int {
	return dev.id
}

// AddUsedResources records the used GPU core and memory
func (dev *DeviceInfo) AddUsedResources(usedCore uint, usedMemory uint) error {
	if usedCore+dev.usedCore > util.HundredCore {
		return fmt.Errorf("update usedcore failed, request: %d, already used: %d",
			usedCore, dev.usedCore)
	}
	if usedMemory+dev.usedMemory > dev.totalMemory {
		return fmt.Errorf("update usedmemory failed, request: %d, already used: %d",
			usedMemory, dev.usedMemory)
	}

	dev.usedCore += usedCore
	dev.usedMemory += usedMemory

	return nil
}

// AllocatableCores returns the remaining cores of this GPU device
func (d *DeviceInfo) AllocatableCores() uint {
	return util.HundredCore - d.usedCore
}

// AllocatableMemory returns the remaining memory of this GPU device
func (d *DeviceInfo) AllocatableMemory() uint {
	return d.totalMemory - d.usedMemory
}

// UsedCores returns the remaining cores of this GPU device
func (d *DeviceInfo) UsedCores() uint {
	return d.usedCore
}

// UsedMemory returns the remaining memory of this GPU device
func (d *DeviceInfo) UsedMemory() uint {
	return d.usedMemory
}

// TotalMemory returns the remaining memory of this GPU device
func (d *DeviceInfo) TotalMemory() uint {
	return d.totalMemory
}
