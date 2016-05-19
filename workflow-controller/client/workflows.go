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
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset/typed/batch/unversioned"

	wapi "k8s.io/contrib/workflow-controller/api"
)

// WorkflowsNamespacer has methods to work with Workflow resources in a namespace
type WorkflowsNamespacer interface {
	Workflows(namespace string) WorkflowInterface
}

// WorkflowInterface exposes methods to work on Workflow resources.
type WorkflowInterface interface {
	List(opts api.ListOptions) (*wapi.WorkflowList, error)
	Get(name string) (*wapi.Workflow, error)
	Create(workflow *wapi.Workflow) (*wapi.Workflow, error)
	Update(workflow *wapi.Workflow) (*wapi.Workflow, error)
	Delete(name string, options *api.DeleteOptions) error
	//	Watch(opts api.ListOptions) (watch.Interface, error)
	//	UpdateStatus(workflow *wapi.Workflow) (*wapi.Workflow, error)
}

// workflows implements WorkflowsNamespacer interface
type workflows struct {
	r  *unversioned.BatchClient
	ns string
}

// newWorkflows returns a workflows
func newWorkflows(c *unversioned.BatchClient, namespace string) *workflows {
	return &workflows{c, namespace}
}

// Ensure statically that workflows implements WorkflowInterface.
var _ WorkflowInterface = &workflows{}

// List returns a list of workflows that match the label and field selectors.
func (c *workflows) List(opts api.ListOptions) (result *wapi.WorkflowList, err error) {
	result = &wapi.WorkflowList{}
	err = nil
	// c.r.Get().Namespace(c.ns).Resource("workflows").VersionedParams(&opts, api.ParameterCodec).Do().Into(result)
	return
}

// Get returns information about a particular workflow.
func (c *workflows) Get(name string) (result *wapi.Workflow, err error) {
	result = &wapi.Workflow{}
	err = nil
	//c.r.Get().Namespace(c.ns).Resource("workflows").Name(name).Do().Into(result)
	return
}

// Create creates a new workflow.
func (c *workflows) Create(workflow *wapi.Workflow) (result *wapi.Workflow, err error) {
	result = &wapi.Workflow{}
	err = nil
	//c.r.Post().Namespace(c.ns).Resource("workflows").Body(workflow).Do().Into(result)
	return
}

// Update updates an existing workflow.
func (c *workflows) Update(workflow *wapi.Workflow) (result *wapi.Workflow, err error) {
	result = &wapi.Workflow{}
	err = nil
	//c.r.Put().Namespace(c.ns).Resource("workflows").Name(workflow.Name).Body(workflow).Do().Into(result)
	return
}

// Delete deletes a workflow, returns error if one occurs.
func (c *workflows) Delete(name string, options *api.DeleteOptions) (err error) {
	return nil // c.r.Delete().Namespace(c.ns).Resource("workflows").Name(name).Body(options).Do().Error()
}

/*
// Watch returns a watch.Interface that watches the requested workflows.
func (c *workflows) Watch(opts api.ListOptions) (watch.Interface, error) {
	return c.r.Get().
		Prefix("watch").
		Namespace(c.ns).
		Resource("workflows").
		VersionedParams(&opts, api.ParameterCodec).
		Watch()
}

// UpdateStatus takes the name of the workflow and the new status.  Returns the server's representation of the workflow, and an error, if it occurs.
func (c *workflows) UpdateStatus(workflow *wapi.Workflow) (result *wapi.Workflow, err error) {
	result = &wapi.Workflow{}
	err = c.r.Put().Namespace(c.ns).Resource("workflows").Name(workflow.Name).SubResource("status").Body(workflow).Do().Into(result)
	return
}
*/
