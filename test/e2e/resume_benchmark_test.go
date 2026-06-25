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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/agent-sandbox/test/e2e/framework"
	"sigs.k8s.io/agent-sandbox/test/e2e/framework/predicates"
)

var SuspendedConditionIsTrue = &predicates.StatusPredicate{
	MatchType:   "Suspended",
	MatchStatus: metav1.ConditionTrue,
}

func TestBenchmarkColdStartVsWarmResume(t *testing.T) {
	tc := framework.NewTestContext(t)
	ctx, cancel := context.WithTimeout(tc.Context(), 10*time.Minute)
	defer cancel()

	ns := &corev1.Namespace{}
	ns.Name = fmt.Sprintf("benchmark-resume-%d", time.Now().UnixNano())
	if err := tc.CreateWithCleanup(ctx, ns); err != nil {
		tc.Fatalf("failed to create namespace: %v", err)
	}

	// 1. Create a SandboxTemplate
	template := &extensionsv1beta1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "late-bind-template",
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
			Name:      "standard-pool",
			Namespace: ns.Name,
		},
		Spec: extensionsv1beta1.SandboxWarmPoolSpec{
			Replicas:    2,
			TemplateRef: extensionsv1beta1.SandboxTemplateRef{Name: template.Name},
		},
	}
	if err := tc.CreateWithCleanup(ctx, warmPool); err != nil {
		tc.Fatalf("failed to create warmpool: %v", err)
	}

	t.Log("Waiting for WarmPool to be ready with replicas...")
	if err := tc.WaitForWarmPoolReady(ctx, types.NamespacedName{Name: warmPool.Name, Namespace: warmPool.Namespace}); err != nil {
		tc.Fatalf("warmpool failed to become ready: %v", err)
	}

	// ----------------------------------------------------
	// SCENARIO 1: Cold Start Latency
	// ----------------------------------------------------
	t.Log("--- Starting Cold Start Latency Test ---")
	coldSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cold-sandbox",
			Namespace: ns.Name,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			OperatingMode: sandboxv1beta1.SandboxOperatingModeRunning,
			PodTemplate: sandboxv1beta1.PodTemplate{
				Spec: template.Spec.PodTemplate.Spec,
			},
		},
	}

	coldStartStart := time.Now()
	if err := tc.CreateWithCleanup(ctx, coldSandbox); err != nil {
		tc.Fatalf("failed to create cold sandbox: %v", err)
	}

	t.Log("Waiting for cold sandbox to be Ready...")
	if err := tc.WaitForSandboxReady(ctx, types.NamespacedName{Name: coldSandbox.Name, Namespace: coldSandbox.Namespace}); err != nil {
		tc.Fatalf("cold sandbox failed to become ready: %v", err)
	}
	coldStartDuration := time.Since(coldStartStart)
	t.Logf("COLD START LATENCY: %s", coldStartDuration)

	// ----------------------------------------------------
	// SCENARIO 2: Warm Resume Latency
	// ----------------------------------------------------
	t.Log("--- Starting Warm Resume Latency Test ---")
	warmSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "warm-sandbox",
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

	if err := tc.CreateWithCleanup(ctx, warmSandbox); err != nil {
		tc.Fatalf("failed to create warm sandbox: %v", err)
	}

	// Wait until it is fully suspended
	t.Log("Waiting for warm sandbox to be fully suspended...")
	err := tc.WaitForObject(ctx, warmSandbox, SuspendedConditionIsTrue)
	if err != nil {
		tc.Fatalf("warm sandbox failed to transition to suspended: %v", err)
	}

	// Now start the clock and resume it!
	t.Log("Resuming warm sandbox...")
	warmResumeStart := time.Now()
	framework.MustUpdateObject(tc.ClusterClient, warmSandbox, func(obj *sandboxv1beta1.Sandbox) {
		obj.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
	})

	t.Log("Waiting for resumed warm sandbox to be Ready...")
	if err := tc.WaitForSandboxReady(ctx, types.NamespacedName{Name: warmSandbox.Name, Namespace: warmSandbox.Namespace}); err != nil {
		tc.Fatalf("resumed sandbox failed to become ready: %v", err)
	}
	warmResumeDuration := time.Since(warmResumeStart)
	t.Logf("WARM RESUME LATENCY: %s", warmResumeDuration)

	// Print comparison summary
	t.Logf("\n=========================================\nLATENCY COMPARISON SUMMARY:\nCold Start:  %s\nWarm Resume: %s\n=========================================\n", coldStartDuration, warmResumeDuration)
}
