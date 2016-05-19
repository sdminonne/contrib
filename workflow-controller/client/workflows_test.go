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

package client_test

/*
func TestListWorkflows(t *testing.T) {
	ns := api.NamespaceAll
	c := &simple.Client{
		Request: simple.Request{
			Method: "GET",
			Path:   testapi.Batch.ResourcePath("workflows", ns, ""),
		},
		Response: simple.Response{
			StatusCode: 200,
			Body: &wapi.WorkflowList{
				Items: []wapi.Workflow{
					{
						ObjectMeta: api.ObjectMeta{
							Name: "mydag",
							Labels: map[string]string{
								"foo": "bar",
							},
						},
						Spec:   wapi.WorkflowSpec{},
						Status: wapi.WorkflowStatus{},
					},
				},
			},
		},
	}
	workflowList, err := c.Setup(t).Batch().Workflows(ns).List(api.ListOptions{})
	defer c.Close()
	c.Validate(t, workflowList, err)
}

func TestGetWorkflow(t *testing.T) {
	ns := api.NamespaceDefault
	c := &simple.Client{
		Request: simple.Request{
			Method: "GET",
			Path:   testapi.Batch.ResourcePath("workflows", ns, "mydag"),
			Query:  simple.BuildQueryValues(nil),
		},
		Response: simple.Response{
			StatusCode: 200,
			Body: &wapi.Workflow{
				ObjectMeta: api.ObjectMeta{
					Name: "mydag",
					Labels: map[string]string{
						"name": "baz",
					},
				},
				Spec:   wapi.WorkflowSpec{},
				Status: wapi.WorkflowStatus{},
			},
		},
	}
	workflow, err := c.Setup(t).Batch().Workflows(ns).Get("mydag")
	defer c.Close()
	c.Validate(t, workflow, err)
}

func TestUpdateWorkflow(t *testing.T) {
	ns := api.NamespaceDefault
	requestWorkflow := &wapi.Workflow{
		ObjectMeta: api.ObjectMeta{
			Name:            "mydag",
			Namespace:       ns,
			ResourceVersion: "1",
		},
	}
	c := &simple.Client{
		Request: simple.Request{
			Method: "PUT",
			Path:   testapi.Batch.ResourcePath("workflows", ns, "mydag"),
			Query:  simple.BuildQueryValues(nil),
		},
		Response: simple.Response{
			StatusCode: 200,
			Body: &wapi.Workflow{
				ObjectMeta: api.ObjectMeta{
					Name: "mydag",
					Labels: map[string]string{
						"foo":  "bar",
						"name": "baz",
					},
				},
				Spec: wapi.WorkflowSpec{},
			},
		},
	}
	receivedWorkflow, err := c.Setup(t).Batch().Workflows(ns).Update(requestWorkflow)
	defer c.Close()
	c.Validate(t, receivedWorkflow, err)
}

func TestUpdateWorkflowStatus(t *testing.T) {
	ns := api.NamespaceDefault
	requestWorkflow := &wapi.Workflow{
		ObjectMeta: api.ObjectMeta{
			Name:            "mydag",
			Namespace:       ns,
			ResourceVersion: "1",
		},
	}
	c := &simple.Client{
		Request: simple.Request{
			Method: "PUT",
			Path:   testapi.Batch.ResourcePath("workflows", ns, "mydag") + "/status",
			Query:  simple.BuildQueryValues(nil),
		},
		Response: simple.Response{
			StatusCode: 200,
			Body: &wapi.Workflow{
				ObjectMeta: api.ObjectMeta{
					Name: "mydag",
					Labels: map[string]string{
						"foo":  "bar",
						"name": "baz",
					},
				},
				Spec:   wapi.WorkflowSpec{},
				Status: wapi.WorkflowStatus{},
			},
		},
	}
	receivedWorkflow, err := c.Setup(t).Batch().Workflows(ns).UpdateStatus(requestWorkflow)
	defer c.Close()
	c.Validate(t, receivedWorkflow, err)
}

func TestDeleteWorkflow(t *testing.T) {
	ns := "testns"
	c := &simple.Client{
		Request: simple.Request{
			Method: "DELETE",
			Path:   testapi.Batch.ResourcePath("workflows", ns, "mydag"),
			Query:  simple.BuildQueryValues(nil),
		},
		Response: simple.Response{StatusCode: 200},
	}
	err := c.Setup(t).Batch().Workflows(ns).Delete("mydag", nil)
	c.Validate(t, nil, err)
}

func TestCreateWorkflow(t *testing.T) {
	ns := "mynamespace"
	requestWorkflow := &wapi.Workflow{
		ObjectMeta: api.ObjectMeta{
			Name:      "mydag",
			Namespace: ns,
		},
	}
	c := &simple.Client{
		Request: simple.Request{
			Method: "POST",
			Path:   testapi.Batch.ResourcePath("workflows", ns, ""),
			Body:   requestWorkflow,
			Query:  simple.BuildQueryValues(nil),
		},
		Response: simple.Response{
			StatusCode: 200,
			Body: &wapi.Workflow{
				ObjectMeta: api.ObjectMeta{
					Name:      "mydag",
					Namespace: "ns",
				},
				Spec: wapi.WorkflowSpec{},
			},
		},
		ResourceGroup: batch.GroupName,
	}
	receivedWorkflow, err := c.Setup(t).Batch().Workflows(ns).Create(requestWorkflow)
	defer c.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c.Validate(t, receivedWorkflow, err)

}
*/
