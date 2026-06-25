// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/agent-sandbox/test/e2e/framework"
	"sigs.k8s.io/agent-sandbox/test/e2e/framework/predicates"
)

func TestBenchmarkScaleWarmResume(t *testing.T) {
	tc := framework.NewTestContext(t)
	
	// Default to batch size of 20 for local runs.
	batchSize := 20
	if val := os.Getenv("SCALE_BATCH_SIZE"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			batchSize = parsed
		}
	}
	
	ctx, cancel := context.WithTimeout(tc.Context(), 15*time.Minute)
	defer cancel()

	ns := &corev1.Namespace{}
	ns.Name = fmt.Sprintf("benchmark-scale-resume-%d", time.Now().UnixNano())
	if err := tc.CreateWithCleanup(ctx, ns); err != nil {
		tc.Fatalf("failed to create namespace: %v", err)
	}

	// 1. Create a SandboxTemplate
	template := &extensionsv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scale-template",
			Namespace: ns.Name,
		},
		Spec: extensionsv1beta1.SandboxTemplateSpec{
			PodTemplate: sandboxv1beta1.PodTemplate{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "pause",
							Image:           "registry.k8s.io/pause:3.10",
							ImagePullPolicy: corev1.PullIfNotPresent,
						},
					},
				},
			},
		},
	}
	if err := tc.CreateWithCleanup(ctx, template); err != nil {
		tc.Fatalf("failed to create template: %v", err)
	}

	// 2. Create a SandboxWarmPool
	warmPool := &extensionsv1beta1.SandboxWarmPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scale-pool",
			Namespace: ns.Name,
		},
		Spec: extensionsv1beta1.SandboxWarmPoolSpec{
			Replicas:    int32(batchSize),
			TemplateRef: extensionsv1beta1.SandboxTemplateRef{Name: template.Name},
		},
	}
	if err := tc.CreateWithCleanup(ctx, warmPool); err != nil {
		tc.Fatalf("failed to create warmpool: %v", err)
	}

	t.Logf("Waiting for WarmPool to be ready with %d replicas...", batchSize)
	if err := tc.WaitForWarmPoolReady(ctx, types.NamespacedName{Name: warmPool.Name, Namespace: warmPool.Namespace}); err != nil {
		tc.Fatalf("warmpool failed to become ready: %v", err)
	}

	// 3. Create N Sandbox CRs in Suspended state
	t.Logf("Creating %d sandboxes in Suspended state...", batchSize)
	sandboxes := make([]*sandboxv1beta1.Sandbox, batchSize)
	for i := range batchSize {
		sbName := fmt.Sprintf("sb-%d", i)
		sandboxes[i] = &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbName,
				Namespace: ns.Name,
				Annotations: map[string]string{
					"agents.x-k8s.io/resume-warm-pool-name": warmPool.Name,
				},
			},
			Spec: sandboxv1beta1.SandboxSpec{
				OperatingMode: sandboxv1beta1.SandboxOperatingModeSuspended,
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: template.Spec.PodTemplate.Spec,
				},
			},
		}
		if err := tc.CreateWithCleanup(ctx, sandboxes[i]); err != nil {
			tc.Fatalf("failed to create sandbox %s: %v", sbName, err)
		}
	}

	// Wait for all sandboxes to be fully suspended
	t.Log("Waiting for all sandboxes to transition to Suspended...")
	var wgReady sync.WaitGroup
	wgReady.Add(batchSize)
	for i := range batchSize {
		go func(sb *sandboxv1beta1.Sandbox) {
			defer wgReady.Done()
			err := tc.WaitForObject(ctx, sb, SuspendedConditionIsTrue)
			if err != nil {
				tc.Errorf("sandbox %s failed to transition to suspended: %v", sb.Name, err)
			}
		}(sandboxes[i])
	}
	wgReady.Wait()
	if t.Failed() {
		tc.Fatalf("aborted because some sandboxes failed to suspend")
	}
	t.Log("All sandboxes are successfully suspended.")

	// 4. Set up watches to record individual transition times
	readyAt := NewConcurrentMap[string, time.Time]()
	var wgWatch sync.WaitGroup
	wgWatch.Add(1)

	go func() {
		defer wgWatch.Done()
		gvr := sandboxv1beta1.GroupVersion.WithResource("sandboxes")
		watchFilter := framework.WatchFilter{
			Namespace: ns.Name,
		}

		framework.MustWatch(ctx, tc.ClusterClient, gvr, watchFilter, func(event watch.Event, obj *sandboxv1beta1.Sandbox) (bool, error) {
			// Only track the user-created sandboxes we want to measure, ignoring standby warm pool sandboxes.
			if len(obj.Name) >= 3 && obj.Name[:3] == "sb-" {
				if matches, err := predicates.ReadyConditionIsTrue.Matches(obj); matches && err == nil {
					if readyAt.PutIfAbsent(obj.Name, time.Now()) {
						t.Logf("User Sandbox %s transitioned to Ready", obj.Name)
					}
				}
			}
			// Stop the watch once we have registered all user sandboxes as ready
			if readyAt.Len() == batchSize {
				return true, nil
			}
			return false, nil
		})
	}()

	// 5. Trigger resumption for all N sandboxes concurrently
	t.Logf("Triggering concurrent resumption of %d sandboxes...", batchSize)
	startTime := time.Now()

	var wgTrigger sync.WaitGroup
	wgTrigger.Add(batchSize)
	for i := range batchSize {
		go func(sb *sandboxv1beta1.Sandbox) {
			defer wgTrigger.Done()
			framework.MustUpdateObject(tc.ClusterClient, sb, func(obj *sandboxv1beta1.Sandbox) {
				obj.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
			})
		}(sandboxes[i])
	}
	wgTrigger.Wait()

	// Wait for the watch to record all completions
	wgWatch.Wait()
	totalWallTime := time.Since(startTime)

	// 6. Compute statistics
	latencies := make([]time.Duration, batchSize)
	var sum time.Duration

	completedReadyAt := readyAt.Snapshot()
	for i := range batchSize {
		sbName := fmt.Sprintf("sb-%d", i)
		timeCompleted, ok := completedReadyAt[sbName]
		if !ok {
			tc.Fatalf("Sandbox %s never completed Ready state watch", sbName)
		}
		duration := timeCompleted.Sub(startTime)
		latencies[i] = duration
		sum += duration
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	minVal := latencies[0]
	maxVal := latencies[batchSize-1]
	p50 := latencies[batchSize/2]
	p90 := latencies[int(float64(batchSize)*0.9)]
	p99 := latencies[int(float64(batchSize)*0.99)]
	mean := sum / time.Duration(batchSize)

	t.Logf("\n=========================================\n"+
		"SCALE PERFORMANCE RESULTS (Batch Size: %d)\n"+
		"Total Wall-Clock Time: %s\n"+
		"Mean Latency:          %s\n"+
		"Min Latency:           %s\n"+
		"Max Latency:           %s\n"+
		"p50 (Median) Latency:  %s\n"+
		"p90 Latency:           %s\n"+
		"p99 Latency:           %s\n"+
		"=========================================\n",
		batchSize, totalWallTime, mean, minVal, maxVal, p50, p90, p99)
}
