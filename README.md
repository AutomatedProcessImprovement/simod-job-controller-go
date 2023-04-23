# simod-job-controller-go

![simod-job-controller](https://github.com/AutomatedProcessImprovement/simod-job-controller-go/actions/workflows/build.yaml/badge.svg)
![version](https://img.shields.io/github/v/tag/AutomatedProcessImprovement/simod-job-controller-go)

This is a worker that listens to `requests.status.pending` messages, starts a job using the Kubernetes API and publishes job statuses back to the message queue.
