package main

import (
	"context"
	"log"

	"github.com/looplab/fsm"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Job struct {
	RequestID string
	Name      string

	brokerChannel *amqp.Channel
	promMetrics   *metrics
	stateMachine  *fsm.FSM
}

func NewJob(requestID, initialState string, promMetrics *metrics, brokerChannel *amqp.Channel) *Job {
	job := &Job{
		RequestID:     requestID,
		brokerChannel: brokerChannel,
		promMetrics:   promMetrics,
	}

	job.stateMachine = fsm.NewFSM(
		initialState,
		fsm.Events{
			{Name: "run", Src: []string{"pending"}, Dst: "running"},
			{Name: "finish", Src: []string{"running"}, Dst: "succeeded"},
			{Name: "fail", Src: []string{"running"}, Dst: "failed"},
		},
		fsm.Callbacks{
			"enter_state": func(_ context.Context, e *fsm.Event) {
				job.enterState(e)
			},
		},
	)

	promMetrics.AddNewJob(initialState, requestID)

	return job
}

func (j *Job) enterState(e *fsm.Event) {
	log.Printf("job %s changes state from %s to %s", j.RequestID, e.Src, e.Dst)
	prometheusMetrics.UpdateJob(e.Src, e.Dst, j.RequestID)
	updateJobStatusHttp(j.RequestID, e.Dst)
}

func (j *Job) SetRunning() error {
	if j.State() == "running" {
		return nil
	}

	return j.stateMachine.Event(context.Background(), "run")
}

func (j *Job) SetRunningWithoutErr() {
	if j.State() == "running" {
		return
	}

	if err := j.stateMachine.Event(context.Background(), "run"); err != nil {
		log.Printf("failed to run job %s: %s", j.RequestID, err)
	}
}

func (j *Job) SetSucceeded() error {
	if j.State() == "succeeded" {
		return nil
	}

	return j.stateMachine.Event(context.Background(), "finish")
}

func (j *Job) SetSucceededWithoutErr() {
	if j.State() == "succeeded" {
		return
	}

	if err := j.stateMachine.Event(context.Background(), "finish"); err != nil {
		log.Printf("failed to finish job %s: %s", j.RequestID, err)
	}
}

func (j *Job) SetFailed() error {
	if j.State() == "failed" {
		return nil
	}

	return j.stateMachine.Event(context.Background(), "fail")
}

func (j *Job) SetFailedWithoutErr() {
	if j.State() == "failed" {
		return
	}

	if err := j.stateMachine.Event(context.Background(), "fail"); err != nil {
		log.Printf("failed to fail job %s: %s", j.RequestID, err)
	}
}

func (j *Job) State() string {
	return j.stateMachine.Current()
}

func (j *Job) SubmitToKubernetes() (jobName string, err error) {
	j.Name, err = submitKubernetesJob(j.RequestID)
	return j.Name, err
}

func (j *Job) Watch() {
	watcherController.Increment()
	watchPodsAndPrepareArchive(j)
	watcherController.Decrement()
}
