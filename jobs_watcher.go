package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	clientCoreV1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

type JobsWatcher struct {
	jobStatuses map[string]string

	mx sync.Mutex
}

func NewJobWatcher() *JobsWatcher {
	return &JobsWatcher{
		jobStatuses: make(map[string]string),
	}
}

func (w *JobsWatcher) Run() {
	jobsClient, err := setupAndMakeJobsClient()
	if err != nil {
		log.Printf("failed to setup jobs client: %s", err)
		return
	}

	podsClient, err := setupAndMakePodsClient()
	if err != nil {
		log.Printf("failed to setup pods client: %s", err)
		return
	}

	ctx := context.Background()
	watcher, err := jobsClient.Watch(ctx, metav1.ListOptions{
		LabelSelector: "app=simod-job",
		Watch:         true,
	})
	if err != nil {
		log.Printf("failed to setup jobs watcher: %s", err)
		return
	}

	defer func() {
		watcher.Stop()
		log.Printf("stopped watching jobs")
	}()

	jobsChan := watcher.ResultChan()

	for event := range jobsChan {
		switch event.Type {
		case watch.Error:
			log.Printf("error watching jobs: %s", event.Object)
		case watch.Deleted:
			job, ok := event.Object.(*batchv1.Job)
			if !ok {
				log.Printf("failed to cast event object to job")
				continue
			}
			log.Printf("job %s was deleted", job.Name)

			requestID := job.Labels["request-id"]
			w.DeleteJob(requestID)
		case watch.Added, watch.Modified:
			job, ok := event.Object.(*batchv1.Job)
			if !ok {
				log.Printf("failed to cast event object to job")
				continue
			}

			status := parseJobStatus(job)

			// to distinguish between pending and running, we query job's pods
			if status == "active" {
				status = getPodStatus(podsClient, job.Name)
			}

			if status == "" {
				continue
			}

			log.Printf("job %s is %s", job.Name, status)

			requestID := job.Labels["request-id"]
			w.HandleJobStatusChange(requestID, status)
		}
	}
}

func (w *JobsWatcher) HandleJobStatusChange(requestID, status string) {
	w.mx.Lock()
	defer w.mx.Unlock()

	previousStatus, ok := w.jobStatuses[requestID]
	if ok && previousStatus == status { // no change
		return
	} else if !ok { // new job
		prometheusMetrics.AddNewJob(status, requestID)
		w.jobStatuses[requestID] = status
	} else { // status changed
		w.jobStatuses[requestID] = status
		prometheusMetrics.UpdateJob(previousStatus, status, requestID)
	}

	if err := patchJobStatus(requestID, status); err != nil {
		log.Printf("failed to patch job status for request %s: %s", requestID, err)
	}

	if status == "succeeded" {
		if _, err := prepareArchive(requestID); err != nil {
			log.Printf("failed to prepare archive for request %s: %s", requestID, err)
		}
	}
}

func (w *JobsWatcher) GetJobStatus(requestID string) string {
	w.mx.Lock()
	defer w.mx.Unlock()

	status, ok := w.jobStatuses[requestID]
	if !ok {
		return ""
	}

	return status
}

func (w *JobsWatcher) DeleteJob(requestID string) {
	w.mx.Lock()
	defer w.mx.Unlock()

	status, ok := w.jobStatuses[requestID]
	if !ok {
		return
	}

	if status == "pending" || status == "running" {
		prometheusMetrics.UpdateJob(status, "failed", requestID)
		
		if err := patchJobStatus(requestID, status); err != nil {
			log.Printf("failed to patch job status for request %s: %s", requestID, err)
		}
	}

	delete(w.jobStatuses, requestID)
}

func parseJobStatus(job *batchv1.Job) string {
	if job.Status.Succeeded > 0 {
		return "succeeded"
	}
	if job.Status.Failed > 0 {
		return "failed"
	}
	if job.Status.Active > 0 {
		return "active"
	}
	return ""
}

func getPodStatus(podsClient clientCoreV1.PodInterface, jobName string) string {
	pods, err := podsClient.List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		log.Printf("failed to list pods for job %s: %s", jobName, err)
		return ""
	}

	statuses := []string{}

	for _, pod := range pods.Items {
		status := parsePodStatus(&pod)
		log.Printf("pod %s for job %s is %s", pod.Name, jobName, status)

		statuses = append(statuses, status)
	}

	if len(statuses) > 1 {
		log.Printf("found more than one pod for job %s", jobName)
	}

	if len(statuses) == 0 {
		log.Printf("found no pods for job %s", jobName)
		return ""
	}

	return statuses[0]
}
