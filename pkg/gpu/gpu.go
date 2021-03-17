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
package gpu

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"

	"sigs.k8s.io/scheduler-plugins/pkg/gpu/algorithm"
	"sigs.k8s.io/scheduler-plugins/pkg/gpu/device"
	"sigs.k8s.io/scheduler-plugins/pkg/gpu/util"
)

const (
	Name           = "GPU"
	ResourceName   = "nvidia.com/gpu"
	NamespaceField = "metadata.namespace"
	NameField      = "metadata.name"
	PodPhaseField  = "status.phase"

	DefaultSkipBindTime = 300 * time.Microsecond
	waitTimeout         = 10 * time.Second
)

type GPU struct {
	FrameworkHandle framework.FrameworkHandle
	kubeClient      kubernetes.Interface
	nodeLister      listerv1.NodeLister
	podLister       listerv1.PodLister
	sync.Mutex
}

var _ framework.FilterPlugin = &GPU{}
var _ framework.ScorePlugin = &GPU{}
var _ framework.ReservePlugin = &GPU{}

// Name returns name of the plugin. It is used in logs, etc.
func (gbp *GPU) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(obj runtime.Object, handle framework.FrameworkHandle) (framework.Plugin, error) {
	kubeClient := handle.ClientSet()

	nodeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)

	podListOptions := func(options *metav1.ListOptions) {
		options.FieldSelector = fmt.Sprintf("%s!=%s", PodPhaseField, corev1.PodSucceeded)
	}
	podInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient,
		time.Second*30, kubeinformers.WithNamespace(metav1.NamespaceAll),
		kubeinformers.WithTweakListOptions(podListOptions))

	gpuFilter := &GPU{
		FrameworkHandle: handle,
		kubeClient:      handle.ClientSet(),
		nodeLister:      nodeInformerFactory.Core().V1().Nodes().Lister(),
		podLister:       podInformerFactory.Core().V1().Pods().Lister(),
	}

	ctx := context.TODO()
	go nodeInformerFactory.Start(ctx.Done())
	go podInformerFactory.Start(ctx.Done())

	return gpuFilter, nil
}

func (g *GPU) Filter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if !util.IsGPURequiredPod(pod) {
		return framework.NewStatus(framework.Success, "skip gpu filter.")
	}

	for k := range pod.Annotations {
		if strings.Contains(k, util.GPUAssigned) ||
			strings.Contains(k, util.PredicateTimeAnnotation) ||
			strings.Contains(k, util.PredicateGPUIndexPrefix) {
			message := fmt.Sprintf("pod %s had been predicated!", pod.Name)
			return framework.NewStatus(framework.Error, message)
		}
	}

	node := nodeInfo.Node()
	if node == nil {
		message := fmt.Sprintf("node %s not found", node.Name)
		return framework.NewStatus(framework.Error, message)
	}

	if !util.IsGPUEnabledNode(node) {
		klog.Infof("no GPU device on node %s", node.Name)
		message := fmt.Sprintf("no GPU device on node %s", node.Name)
		return framework.NewStatus(framework.Error, message)
	}

	pods, err := g.ListPodsOnNode(node)
	if err != nil {
		message := fmt.Sprintf("failed to get pods on node: %s", node.Name)
		return framework.NewStatus(framework.Error, message)
	}

	gpuNodeInfo := device.NewNodeInfo(node, pods)
	alloc := algorithm.NewAllocator(gpuNodeInfo)
	err = alloc.Allocate(pod)
	if err != nil {
		message := fmt.Sprintf("pod %s does not match with this node", pod.Name)
		return framework.NewStatus(framework.Error, message)
	}

	return framework.NewStatus(framework.Success, "filter done.")
}

func (g *GPU) Score(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) (int64, *framework.Status) {
	if !util.IsGPURequiredPod(pod) {
		return 0, framework.NewStatus(framework.Success, "skip gpu score.")
	}

	nodeInfo, err := g.FrameworkHandle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		klog.Errorf("get nodes info err: %v.", err)
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	node := nodeInfo.Node()
	if node == nil {
		return 0, framework.NewStatus(framework.Error, "node not found")
	}

	pods, err := g.ListPodsOnNode(node)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, "failed to get pods on node")
	}

	gpuNodeInfo := device.NewNodeInfo(node, pods)
	alloc := algorithm.NewAllocator(gpuNodeInfo)
	s := alloc.Score(pod)

	return s, framework.NewStatus(framework.Success, "score done.")
}

func (gbp *GPU) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (g *GPU) Reserve(_ context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	if !util.IsGPURequiredPod(pod) {
		return framework.NewStatus(framework.Success, "skip gpu reserve.")
	}

	nodeInfo, err := g.FrameworkHandle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		klog.Errorf("get nodes info err: %v.", err)
		return framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	pods, err := g.ListPodsOnNode(node)
	if err != nil {
		return framework.NewStatus(framework.Error, "failed to get pods on node")
	}

	gpuNodeInfo := device.NewNodeInfo(node, pods)
	alloc := algorithm.NewAllocator(gpuNodeInfo)
	newPod, err := alloc.Reserve(pod)
	if err != nil {
		message := fmt.Sprintf("pod %s does not match with this node", pod.Name)
		return framework.NewStatus(framework.Error, message)
	} else {
		annotationMap := make(map[string]string)
		for k, v := range newPod.Annotations {
			if strings.Contains(k, util.GPUAssigned) ||
				strings.Contains(k, util.PredicateTimeAnnotation) ||
				strings.Contains(k, util.PredicateGPUIndexPrefix) ||
				strings.Contains(k, util.PredicateNode) {
				annotationMap[k] = v
			}
		}
		err := g.patchPodWithAnnotations(newPod, annotationMap)
		if err != nil {
			message := fmt.Sprintf("update pod annotation failed")
			return framework.NewStatus(framework.Error, message)
		}
	}
	return framework.NewStatus(framework.Success, "")
}

func (g *GPU) Unreserve(_ context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	return
}

func (g *GPU) ListPodsOnNode(node *corev1.Node) ([]*corev1.Pod, error) {
	// #lizard forgives
	pods, err := g.podLister.Pods(corev1.NamespaceAll).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var ret []*corev1.Pod
	for _, pod := range pods {
		klog.V(9).Infof("List pod %s", pod.Name)
		if !util.IsGPURequiredPod(pod) {
			continue
		}
		var predicateNode string
		if pod.Spec.NodeName == "" && pod.Annotations != nil {
			if v, ok := pod.Annotations[util.PredicateNode]; ok {
				predicateNode = v
			}
		}
		if (pod.Spec.NodeName == node.Name || predicateNode == node.Name) &&
			pod.Status.Phase != corev1.PodSucceeded &&
			pod.Status.Phase != corev1.PodFailed {
			ret = append(ret, pod)
			klog.V(9).Infof("get pod %s on node %s", pod.UID, node.Name)
		}
	}
	return ret, nil
}

func (g *GPU) patchPodWithAnnotations(
	pod *corev1.Pod, annotationMap map[string]string) error {
	// update annotations by patching to the pod
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
	}
	payload := patchPod{
		Metadata: patchMetadata{
			Annotations: annotationMap,
		},
	}

	payloadBytes, _ := json.Marshal(payload)
	err := wait.PollImmediate(time.Second, waitTimeout, func() (bool, error) {
		_, err := g.kubeClient.CoreV1().Pods(pod.Namespace).
			Patch(context.TODO(), pod.Name, k8stypes.StrategicMergePatchType, payloadBytes, metav1.PatchOptions{})
		if err == nil {
			return true, nil
		}
		if util.ShouldRetry(err) {
			return false, nil
		}

		return false, err
	})
	if err != nil {
		msg := fmt.Sprintf("failed to add annotation %v to pod %s due to %s",
			annotationMap, pod.UID, err.Error())
		klog.Infof(msg)
		return fmt.Errorf(msg)
	}
	return nil
}
