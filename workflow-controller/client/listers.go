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

package client

import (
	"fmt"

	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/apis/batch"
	kapi "k8s.io/kubernetes/pkg/api"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/labels"

	wapi "k8s.io/contrib/workflow-controller/api"
)

// StoreToJobLister gives a store List and Exists methods. The store must contain only Jobs.
type StoreToJobLister struct {
	kcache.Store
}

// Exists checks if the given job exists in the store.
func (s *StoreToJobLister) Exists(job *batch.Job) (bool, error) {
	_, exists, err := s.Store.Get(job)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// List lists all jobs in the store.
func (s *StoreToJobLister) List() (jobs batch.JobList, err error) {
	for _, c := range s.Store.List() {
		jobs.Items = append(jobs.Items, *(c.(*batch.Job)))
	}
	return jobs, nil
}

// Jobs will look like the api pkg/client
func (s *StoreToJobLister) Jobs(namespace string) StoreJobsNamespacer {
	return StoreJobsNamespacer{s.Store, namespace}
}

// StoreJobsNamespacer represents store for jobs
type StoreJobsNamespacer struct {
	store     kcache.Store
	namespace string
}

// List returns a list of jobs fromt he store
func (s StoreJobsNamespacer) List(selector labels.Selector) (jobs batch.JobList, err error) {
	list := batch.JobList{}
	for _, m := range s.store.List() {
		job := m.(*batch.Job)
		if s.namespace == kapi.NamespaceAll || s.namespace == job.Namespace {
			if selector.Matches(labels.Set(job.Labels)) {
				list.Items = append(list.Items, *job)
			}
		}
	}
	return list, nil
}

// GetPodJobs returns a list of jobs managing a pod. Returns an error only if no matching jobs are found.
func (s *StoreToJobLister) GetPodJobs(pod *kapi.Pod) (jobs []batch.Job, err error) {
	var selector labels.Selector
	var job batch.Job

	if len(pod.Labels) == 0 {
		err = fmt.Errorf("no jobs found for pod %v because it has no labels", pod.Name)
		return
	}

	for _, m := range s.Store.List() {
		job = *m.(*batch.Job)
		if job.Namespace != pod.Namespace {
			continue
		}

		selector, _ = unversioned.LabelSelectorAsSelector(job.Spec.Selector)
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		jobs = append(jobs, job)
	}
	if len(jobs) == 0 {
		err = fmt.Errorf("could not find jobs for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}
	return
}

// StoreToWorkflowLister gives a store List and Exists methods. The store must contain only Workflows.
type StoreToWorkflowLister struct {
	kcache.Store
}

// Exists returns true if a Workflow is present in the store
func (s *StoreToWorkflowLister) Exists(workflow *wapi.Workflow) (bool, error) {
	_, exists, err := s.Store.Get(workflow)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// List lists all workflows in the store.
func (s *StoreToWorkflowLister) List() (workflows wapi.WorkflowList, err error) {
	for _, c := range s.Store.List() {
		workflows.Items = append(workflows.Items, *(c.(*wapi.Workflow)))
	}
	return workflows, nil
}

// GetJobWorkflows return the Workflows which may control the job
func (s *StoreToWorkflowLister) GetJobWorkflows(job *batch.Job) (workflows []wapi.Workflow, err error) {
	var selector labels.Selector
	var workflow wapi.Workflow

	if len(job.Labels) == 0 {
		err = fmt.Errorf("no workflows found for job %v because it has no labels", job.Name)
		return
	}
	for _, m := range s.Store.List() {
		workflow = *m.(*wapi.Workflow)
		if workflow.Namespace != job.Namespace {
			continue
		}
		selector, _ = unversioned.LabelSelectorAsSelector(job.Spec.Selector)
		if selector.Matches(labels.Set(job.Labels)) {
			workflows = append(workflows, workflow)
		}
	}
	if len(workflows) == 0 {
		err = fmt.Errorf("could not find workflows for job %s in namespace %s with labels: %v", job.Name, job.Namespace, job.Labels)
	}
	return
}
