/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package validation

import (
	"strings"
	"testing"
	"time"

	wapi "k8s.io/contrib/workflow-controller/api"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/types"
)

func TestValidateWorkflowSpec(t *testing.T) {
	successCases := map[string]wapi.Workflow{
		"K1": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {
						JobTemplate: &batch.JobTemplateSpec{},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"2K1": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
					"two": {JobTemplate: &batch.JobTemplateSpec{}},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"K2": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
					"two": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"one"},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"2K2": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
					"two": {JobTemplate: &batch.JobTemplateSpec{},
						Dependencies: []string{"one"},
					},
					"three": {JobTemplate: &batch.JobTemplateSpec{}},
					"four": {JobTemplate: &batch.JobTemplateSpec{},
						Dependencies: []string{"three"},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"K3": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
					"two": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"one"},
					},
					"three": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"one", "two"},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
	}
	for k, v := range successCases {
		errs := ValidateWorkflow(&v)
		if len(errs) != 0 {
			t.Errorf("%s unexpected error %v", k, errs)
		}
	}
	negative64 := int64(-42)
	errorCases := map[string]wapi.Workflow{
		"spec.steps: Forbidden: detected cycle [one]": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{},
						Dependencies: []string{"one"},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"spec.steps: Forbidden: detected cycle [two one]": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"two"},
					},
					"two": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"one"},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"spec.steps: Forbidden: detected cycle [three four five]": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
					"two": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"one"},
					},
					"three": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"two", "five"},
					},
					"four": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"three"},
					},
					"five": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"four"},
					},
					"six": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"five"},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"spec.steps: Not found: \"three\"": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
					"two": {
						JobTemplate:  &batch.JobTemplateSpec{},
						Dependencies: []string{"three"},
					},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"spec.activeDeadlineSeconds: Invalid value: -42: must be greater than or equal to 0": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
				},
				ActiveDeadlineSeconds: &negative64,
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
		"spec.selector: Required value": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {JobTemplate: &batch.JobTemplateSpec{}},
				},
			},
		},
		"spec.steps.jobTemplate: Required value": {
			ObjectMeta: kapi.ObjectMeta{
				Name:      "mydag",
				Namespace: kapi.NamespaceDefault,
				UID:       types.UID("1uid1cafe"),
			},
			Spec: wapi.WorkflowSpec{
				Steps: map[string]wapi.WorkflowStep{
					"one": {},
				},
				Selector: &unversioned.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
	}

	for k, v := range errorCases {
		errs := ValidateWorkflow(&v)
		if len(errs) == 0 {
			t.Errorf("expected failure for %s", k)
		} else {
			s := strings.Split(k, ":")
			err := errs[0]
			if err.Field != s[0] || !strings.Contains(err.Error(), s[1]) {
				t.Errorf("unexpected error: %v, expected: %s", err, k)
			}
		}
	}
}

func NewWorkflow() wapi.Workflow {
	return wapi.Workflow{
		ObjectMeta: kapi.ObjectMeta{
			Name:            "mydag",
			Namespace:       kapi.NamespaceDefault,
			UID:             types.UID("1uid1cafe"),
			ResourceVersion: "42",
		},
		Spec: wapi.WorkflowSpec{
			Steps: map[string]wapi.WorkflowStep{
				"one": {
					JobTemplate: &batch.JobTemplateSpec{},
				},
				"two": {
					JobTemplate: &batch.JobTemplateSpec{},
				},
			},
			Selector: &unversioned.LabelSelector{
				MatchLabels: map[string]string{"a": "b"},
			},
		},
		Status: wapi.WorkflowStatus{
			StartTime: &unversioned.Time{time.Date(2009, time.January, 1, 27, 6, 25, 0, time.UTC)},
			Statuses: map[string]wapi.WorkflowStepStatus{
				"one": {
					Complete: false,
				},
				"two": {
					Complete: false,
				},
			},
		},
	}
}

func TestValidateWorkflowUpdate(t *testing.T) {

	type WorkflowPair struct {
		current      wapi.Workflow
		patchCurrent func(*wapi.Workflow)
		update       wapi.Workflow
		patchUpdate  func(*wapi.Workflow)
	}
	errorCases := map[string]WorkflowPair{
		"metadata.resourceVersion: Invalid value: \"\": must be specified for an update": {
			current: NewWorkflow(),
			update:  NewWorkflow(),
			patchUpdate: func(w *wapi.Workflow) {
				w.ObjectMeta.ResourceVersion = ""
			},
		},
		"workflow: Forbidden: cannot update completed workflow": {
			current: NewWorkflow(),
			patchCurrent: func(w *wapi.Workflow) {
				s1 := w.Status.Statuses["one"]
				s1.Complete = true // one is complete
				w.Status.Statuses["one"] = s1
				s2 := w.Status.Statuses["two"]
				s2.Complete = true // two is complete
				w.Status.Statuses["two"] = s2
			},
			update: NewWorkflow(),
		},
		"spec.steps: Forbidden: cannot delete running step \"one\"": {
			current: NewWorkflow(),
			patchCurrent: func(w *wapi.Workflow) {
				delete(w.Status.Statuses, "two") // one is running
			},
			update: NewWorkflow(),
			patchUpdate: func(w *wapi.Workflow) {
				// we delete "one"
				delete(w.Spec.Steps, "one") // trying to remove a running step
				delete(w.Status.Statuses, "two")
			},
		},
		"spec.steps: Forbidden: cannot modify running step \"one\"": {
			current: NewWorkflow(),
			patchCurrent: func(w *wapi.Workflow) {
				delete(w.Status.Statuses, "two") // one is running
			},
			update: NewWorkflow(),
			patchUpdate: func(w *wapi.Workflow) {
				// modify "one"
				s := w.Spec.Steps["one"]
				s.JobTemplate.Labels = make(map[string]string)
				s.JobTemplate.Labels["foo"] = "bar"
				w.Spec.Steps["one"] = s
				delete(w.Status.Statuses, "two")
			},
		},
		"spec.steps: Forbidden: cannot delete completed step \"one\"": {
			current: NewWorkflow(),
			patchCurrent: func(w *wapi.Workflow) {
				s := w.Status.Statuses["one"]
				s.Complete = true // one is complete
				w.Status.Statuses["one"] = s
				delete(w.Status.Statuses, "two") // two is running
			},
			update: NewWorkflow(),
			patchUpdate: func(w *wapi.Workflow) {
				delete(w.Spec.Steps, "one") // removing a complete step
			},
		},
		"spec.steps: Forbidden: cannot modify completed step \"one\"": {
			current: NewWorkflow(),
			patchCurrent: func(w *wapi.Workflow) {
				s := w.Status.Statuses["one"]
				s.Complete = true // one is complete
				w.Status.Statuses["one"] = s
				delete(w.Status.Statuses, "two") // two is running
			},
			update: NewWorkflow(),
			patchUpdate: func(w *wapi.Workflow) {
				// modify "one"
				s := w.Spec.Steps["one"]
				s.JobTemplate.Labels = make(map[string]string)
				s.JobTemplate.Labels["foo"] = "bar"
				w.Spec.Steps["one"] = s
				delete(w.Status.Statuses, "two") // two always running
			},
		},
	}
	for k, v := range errorCases {
		if v.patchUpdate != nil {
			v.patchUpdate(&v.update)
		}
		if v.patchCurrent != nil {
			v.patchCurrent(&v.current)
		}
		errs := ValidateWorkflowUpdate(&v.update, &v.current)
		if len(errs) == 0 {
			t.Errorf("expected failure for %s", k)
			continue
		}
		if errs.ToAggregate().Error() != k {
			t.Errorf("unexpected error: %v, expected: %s", errs, k)
		}
	}
}
