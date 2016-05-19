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

package workflow

import (
	"reflect"
	"time"
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/client/cache"
	clientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	unversionedcore "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset/typed/core/unversioned"
	"k8s.io/kubernetes/pkg/client/record"
	client "k8s.io/kubernetes/pkg/client/unversioned" // @sdminonne: TODO: remove it
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/framework"
	//jobcontroller "k8s.io/kubernetes/pkg/controller/job"
	replicationcontroller "k8s.io/kubernetes/pkg/controller/replication"
	"k8s.io/kubernetes/pkg/runtime"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/workqueue"
	"k8s.io/kubernetes/pkg/watch"
	"k8s.io/kubernetes/pkg/apis/batch"

	wapi "k8s.io/contrib/workflow-controller/api"
	wclient "k8s.io/contrib/workflow-controller/client"
)


// WorkflowStepLabelKey defines the key of label to be injected by workflow controller 
const WorkflowStepLabelKey = "kubernetes.io/workflow"

// Workflow implements the controller for Workflow
type Workflow struct {

	// @sdminonne: TODO: kubeClient should be clientset.Interface
	oldKubeClient client.Interface

	kubeClient clientset.Interface

	jobControl JobControlInterface

	// To allow injection of updateWorkflowStatus for testing.
	updateHandler func(workflow *wapi.Workflow) error
	syncHandler   func(workflowKey string) error

	// jobStoreSynced returns true if the jod store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	jobStoreSynced func() bool

	// A TTLCache of job creates/deletes each rc expects to see
	expectations controller.ControllerExpectationsInterface

	// A store of workflow, populated by the frameworkController
	workflowStore wclient.StoreToWorkflowLister
	// Watches changes to all workflows
	Workflow *framework.Controller

	// Store of job
	jobStore wclient.StoreToJobLister
	// Watches changes to all jobs
	jobController *framework.Controller

	// Workflows that need to be updated
	queue *workqueue.Type

	recorder record.EventRecorder
}

// NewWorkflow creates and initializes a Workflow
func NewWorkflow(oldClient client.Interface, kubeClient clientset.Interface, resyncPeriod controller.ResyncPeriodFunc) *Workflow {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&unversionedcore.EventSinkImpl{Interface: kubeClient.Core().Events("")})

	wc := &Workflow{
		oldKubeClient: oldClient,
		kubeClient:    kubeClient,
		jobControl: WorkflowJobControl{
			KubeClient: kubeClient,
			Recorder:   eventBroadcaster.NewRecorder(api.EventSource{Component: "workflow-controller"}),
		},
		expectations: controller.NewControllerExpectations(),
		queue:        workqueue.New(),
		recorder:     eventBroadcaster.NewRecorder(api.EventSource{Component: "workflow-controller"}),
	}

	wc.workflowStore.Store, wc.Workflow = framework.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (runtime.Object, error) {
				//return wc.oldKubeClient.Batch().Workflows(api.NamespaceAll).List(options)
				return &wapi.WorkflowList{}, nil
				
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				//return wc.oldKubeClient.Batch().Workflows(api.NamespaceAll).Watch(options)
				return nil , nil
			},
		},
		&wapi.Workflow{},
		replicationcontroller.FullControllerResyncPeriod,
		framework.ResourceEventHandlerFuncs{
			AddFunc: wc.enqueueController,
			UpdateFunc: func(old, cur interface{}) {
				if workflow := cur.(*wapi.Workflow); !isWorkflowFinished(workflow) {
					wc.enqueueController(workflow)
				}
			},
			DeleteFunc: wc.enqueueController,
		},
	)

	wc.jobStore.Store, wc.jobController = framework.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (runtime.Object, error) {
				return wc.oldKubeClient.Batch().Jobs(api.NamespaceAll).List(options)
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return wc.oldKubeClient.Batch().Jobs(api.NamespaceAll).Watch(options)
			},
		},
		&batch.Job{},
		replicationcontroller.FullControllerResyncPeriod,
		framework.ResourceEventHandlerFuncs{
			AddFunc:    wc.addJob,
			UpdateFunc: wc.updateJob,
			DeleteFunc: wc.deleteJob,
		},
	)

	wc.updateHandler = wc.updateWorkflowStatus
	wc.syncHandler = wc.syncWorkflow
	wc.jobStoreSynced = wc.jobController.HasSynced
	return wc
}

// Run the main goroutine responsible for watching and syncing workflows.
func (w *Workflow) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	go w.Workflow.Run(stopCh)
	go w.jobController.Run(stopCh)
	for i := 0; i < workers; i++ {
		go wait.Until(w.worker, time.Second, stopCh)
	}
	<-stopCh
	glog.Infof("Shutting down Workflow Controller")
	w.queue.ShutDown()
}

// getJobWorkflow return the workflow managing the given job
func (w *Workflow) getJobWorkflow(job *batch.Job) *wapi.Workflow {
	workflows, err := w.workflowStore.GetJobWorkflows(job)
	if err != nil {
		glog.V(4).Infof("No workflows found for job %v: %v", job.Name, err)
		return nil
	}
	if len(workflows) > 1 {
		glog.Errorf("user error! more than one workflow is selecting jobs with labels: %+v", job.Labels)
		//sort.Sort(byCreationTimestamp(jobs))
	}
	return &workflows[0]
}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
func (w *Workflow) worker() {
	for {
		func() {
			key, quit := w.queue.Get()
			if quit {
				return
			}
			defer w.queue.Done(key)
			err := w.syncHandler(key.(string))
			if err != nil {
				glog.Errorf("Error syncing workflow: %v", err)
			}
		}()
	}
}

func (w *Workflow) syncWorkflow(key string) error {
	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing workflow %q (%v)", key, time.Now().Sub(startTime))
	}()

	if !w.jobStoreSynced() {
		time.Sleep(100 * time.Millisecond) // @sdminonne: TODO remove hard coded value
		glog.Infof("Waiting for job controller to sync, requeuing workflow %v", key)
		w.queue.Add(key)
		return nil
	}

	obj, exists, err := w.workflowStore.Store.GetByKey(key)
	if !exists {
		glog.V(4).Infof("Workflow has been deleted: %v", key)
		w.expectations.DeleteExpectations(key)
		return nil
	}
	if err != nil {
		glog.Errorf("Unable to retrieve workflow %v from store: %v", key, err)
		w.queue.Add(key)
		return err
	}
	workflow := *obj.(*wapi.Workflow)
	workflowKey, err := controller.KeyFunc(&workflow)
	if err != nil {
		glog.Errorf("Couldn't get key for workflow %#v: %v", workflow, err)
		return err
	}

	if workflow.Status.Statuses == nil {
		workflow.Status.Statuses = make(map[string]wapi.WorkflowStepStatus, len(workflow.Spec.Steps))
		now := unversioned.Now()
		workflow.Status.StartTime = &now
	}

	workflowNeedsSync := w.expectations.SatisfiedExpectations(workflowKey)
	if !workflowNeedsSync {
		glog.V(4).Infof("Workflow %v doesn't need synch", workflow.Name)
		return nil
	}

	if isWorkflowFinished(&workflow) {
		return nil
	}

	if pastActiveDeadline(&workflow) {
		// @sdminonne: TODO delete jobs & write error for the ExternalReference
		now := unversioned.Now()
		condition := wapi.WorkflowCondition{
			Type:               wapi.WorkflowFailed,
			Status:             api.ConditionTrue,
			LastProbeTime:      now,
			LastTransitionTime: now,
			Reason:             "DeadlineExceeded",
			Message:            "Workflow was active longer than specified deadline",
		}
		workflow.Status.Conditions = append(workflow.Status.Conditions, condition)
		workflow.Status.CompletionTime = &now
		w.recorder.Event(&workflow, api.EventTypeNormal, "DeadlineExceeded", "Workflow was active longer than specified deadline")
		if err := w.updateHandler(&workflow); err != nil {
			glog.Errorf("Failed to update workflow %v, requeuing.  Error: %v", workflow.Name, err)
			w.enqueueController(&workflow)
		}
		return nil
	}

	if w.manageWorkflow(&workflow) {
		if err := w.updateHandler(&workflow); err != nil {
			glog.Errorf("Failed to update workflow %v, requeuing.  Error: %v", workflow.Name, err)
			w.enqueueController(&workflow)
		}
	}
	return nil
}

// pastActiveDeadline checks if workflow has ActiveDeadlineSeconds field set and if it is exceeded.
func pastActiveDeadline(workflow *wapi.Workflow) bool {
	if workflow.Spec.ActiveDeadlineSeconds == nil || workflow.Status.StartTime == nil {
		return false
	}
	now := unversioned.Now()
	start := workflow.Status.StartTime.Time
	duration := now.Time.Sub(start)
	allowedDuration := time.Duration(*workflow.Spec.ActiveDeadlineSeconds) * time.Second
	return duration >= allowedDuration
}

func (w *Workflow) updateWorkflowStatus(workflow *wapi.Workflow) error {
	//_, err := w.oldKubeClient.Batch().Workflows(workflow.Namespace).UpdateStatus(workflow)
	// todo @sdminonne: client to support UpdateStatus
	return nil
}

func isWorkflowFinished(w *wapi.Workflow) bool {
	for _, c := range w.Status.Conditions {
		if (c.Type == wapi.WorkflowComplete || c.Type == wapi.WorkflowFailed) && c.Status == api.ConditionTrue {
			return true
		}
	}
	return false
}

func (w *Workflow) enqueueController(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		glog.Errorf("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	w.queue.Add(key)
}

func (w *Workflow) addJob(obj interface{}) {
	job := obj.(*batch.Job)
	glog.V(4).Infof("addJob %v", job.Name)
	if workflow := w.getJobWorkflow(job); workflow != nil {
		key, err := controller.KeyFunc(workflow)
		if err != nil {
			glog.Errorf("No key for workflow %#v: %v", workflow, err)
			return
		}
		w.expectations.CreationObserved(key)
		w.enqueueController(workflow)
	}
}

func (w *Workflow) updateJob(old, cur interface{}) {
	oldJob := old.(*batch.Job)
	curJob := cur.(*batch.Job)
	glog.V(4).Infof("updateJob old=%v, cur=%v ", oldJob.Name, curJob.Name)
	if api.Semantic.DeepEqual(old, cur) {
		glog.V(4).Infof("\t nothing to update")
		return
	}
	if workflow := w.getJobWorkflow(curJob); workflow != nil {
		w.enqueueController(workflow)
	}
	// in case of relabelling
	if !reflect.DeepEqual(oldJob.Labels, curJob.Labels) {
		if oldWorkflow := w.getJobWorkflow(oldJob); oldWorkflow != nil {
			w.enqueueController(oldWorkflow)
		}
	}
}

func (w *Workflow) deleteJob(obj interface{}) {
	job, ok := obj.(*batch.Job)
	glog.V(4).Infof("deleteJob old=%v", job.Name)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			glog.Errorf("Couldn't get object from tombstone %+v, could take up to %v before a workflow recreates a job", obj, controller.ExpectationsTimeout)
			return
		}
		job, ok = tombstone.Obj.(*batch.Job)
		if !ok {
			glog.Errorf("Tombstone contained object that is not a job %+v, could take up to %v before a workflow recreates a job", obj, controller.ExpectationsTimeout)
			return
		}
	}
	if workflow := w.getJobWorkflow(job); workflow != nil {
		key, err := controller.KeyFunc(obj)
		if err != nil {
			glog.Errorf("Couldn't get key for workflow %#v: %v", workflow, err)
			return
		}
		w.expectations.DeletionObserved(key)
		w.enqueueController(workflow)
	}
}

func (w *Workflow) manageWorkflow(workflow *wapi.Workflow) bool {
	needsStatusUpdate := false
	glog.V(4).Infof("manage Workflow -> %v", workflow.Name)
	workflowComplete := true
	for stepName, step := range workflow.Spec.Steps {
		if stepStatus, ok := workflow.Status.Statuses[stepName]; ok && stepStatus.Complete {
			continue // step completed nothing to do
		}
		workflowComplete = false
		switch {
		case step.JobTemplate != nil: // Job step
			needsStatusUpdate = w.manageWorkflowJob(workflow, stepName, &step) || needsStatusUpdate
		//case step.ExternalRef != nil: // external object reference
		//	needsStatusUpdate = w.manageWorkflowReference(workflow, stepName, &step) || needsStatusUpdate
		}
	}

	if workflowComplete {
		now := unversioned.Now()
		condition := wapi.WorkflowCondition{
			Type:               wapi.WorkflowComplete,
			Status:             api.ConditionTrue,
			LastProbeTime:      now,
			LastTransitionTime: now,
		}
		workflow.Status.Conditions = append(workflow.Status.Conditions, condition)
		workflow.Status.CompletionTime = &now
		needsStatusUpdate = true
	}

	return needsStatusUpdate
}

func (w *Workflow) manageWorkflowJob(workflow *wapi.Workflow, stepName string, step *wapi.WorkflowStep) bool {
	for _, dependcyName := range step.Dependencies {
		if dependencyStatus, ok := workflow.Status.Statuses[dependcyName]; !ok || !dependencyStatus.Complete {
			glog.V(4).Infof("Dependecy %v not satisfied for %v", dependcyName, stepName)
			return false
		}
	}

	// all dependency satisfied (or missing) need action: update or create step
	key, err := controller.KeyFunc(workflow)
	if err != nil {
		glog.Errorf("Couldn't get key for workflow %#v: %v", workflow, err)
		return false
	}
	// fetch job by labelSelector and step
	jobSelector := CreateWorkflowJobLabelSelector(workflow, workflow.Spec.Steps[stepName].JobTemplate, stepName)
	jobList, err := w.jobStore.Jobs(workflow.Namespace).List(jobSelector)
	if err != nil {
		glog.Errorf("Error getting jobs for workflow %q: %v", key, err)
		w.queue.Add(key)
		return false
	}

	switch len(jobList.Items) {
	case 0: // create job
		err := w.jobControl.CreateJob(workflow.Namespace, step.JobTemplate, workflow, stepName)
		if err != nil {
			defer utilruntime.HandleError(err)
			w.expectations.CreationObserved(key)
		}
	case 1: // update status
		job := jobList.Items[0]
		reference, err := api.GetReference(&job)
		if err != nil || reference == nil {
			glog.Errorf("Unable to get reference from %v: %v", job.Name, err)
			return false
		}
		jobFinished := IsJobFinished(&job)
		workflow.Status.Statuses[stepName] = wapi.WorkflowStepStatus{
			Complete:  jobFinished,
			Reference: *reference}
	default: // reconciliate
		glog.Errorf("Workflow.manageWorkfloJob %v too many jobs reported... Need reconciliation", workflow.Name)
		return false
	}
	return true
}

// IsJobFinished returns true whether or not a job is finished
func IsJobFinished(j *batch.Job) bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == batch.JobComplete || c.Type == batch.JobFailed) && c.Status == api.ConditionTrue {
			return true
		}
	}
	return false
}