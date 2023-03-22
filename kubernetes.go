package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/walle/targz"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	clientBatchV1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	clientCoreV1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func submitKubernetesJob(request pendingRequest) (jobName string, err error) {
	log.Printf("submitting job for %s", request.RequestID)

	jobsClient, err := setupAndMakeJobsClient()
	if err != nil {
		return "", fmt.Errorf("failed to setup jobs client: %s", err)
	}

	resultsOutputDir := path.Join(requestsBaseDir, request.RequestID, "results")
	err = os.MkdirAll(resultsOutputDir, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create results output dir at %s: %s", resultsOutputDir, err)
	}

	job := makeJobForRequest(request.RequestID, request.ConfigurationPath, resultsOutputDir)
	if job == nil {
		return "", fmt.Errorf("failed to create job for %s", request.RequestID)
	}

	ctx := context.Background()
	_, err = jobsClient.Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create job for %s: %s", request.RequestID, err)
	}

	return job.Name, err
}

func setupAndMakeJobsClient() (clientBatchV1.JobInterface, error) {
	clientset, err := setupKubernetesIfNotSet()
	if err != nil {
		return nil, fmt.Errorf("failed to setup kubernetes: %s", err)
	}

	jobsClient := clientset.BatchV1().Jobs(kubernetesNamespace)

	return jobsClient, nil
}

func setupAndMakePodsClient() (clientCoreV1.PodInterface, error) {
	clientset, err := setupKubernetesIfNotSet()
	if err != nil {
		return nil, fmt.Errorf("failed to setup kubernetes: %s", err)
	}

	podsClient := clientset.CoreV1().Pods(kubernetesNamespace)

	return podsClient, nil
}

func setupKubernetesIfNotSet() (*kubernetes.Clientset, error) {
	if kubernetesClientset != nil {
		return kubernetesClientset, nil
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubernetes config: %s", err)
	}

	kubernetesClientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %s", err)
	}

	return kubernetesClientset, nil
}

func makeJobForRequest(requestId, configPath string, resultsOutputDir string) *batchv1.Job {
	backoffLimit := int32(0)

	// It's important to set ttlSeconds to 0, otherwise finished and hanging jobs will be recorded with Prometheus several times.
	// This can be resolved if a permanent storage for Simod Jobs is set up.
	ttlSeconds := int32(180)

	jobName := jobNameFromRequestId(requestId)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: kubernetesNamespace,
			Labels: map[string]string{
				"app":        "simod-job",
				"request-id": requestId,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSeconds,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "simod",
							Image: simodDockerImage,
							Args:  []string{"bash", "run.sh", configPath, resultsOutputDir},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "simod-data",
									MountPath: "/tmp/simod-volume",
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(simodJobResourceCpuRequest),
									corev1.ResourceMemory: resource.MustParse(simodJobResourceMemoryRequest),
								},
							},
							TerminationMessagePath: fmt.Sprintf("%s/%s/terminationMessage.txt", requestsBaseDir, requestId),
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Volumes: []corev1.Volume{
						{
							Name: "simod-data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "simod-volume-claim",
								},
							},
						},
					},
				},
			},
		},
	}
	return job
}

func jobNameFromRequestId(requestId string) string {
	return fmt.Sprintf("simod-%s", requestId)
}

func parsePodStatus(pod *corev1.Pod) string {
	if pod.Status.Phase == corev1.PodSucceeded {
		return "succeeded"
	} else if pod.Status.Phase == corev1.PodFailed {
		return "failed"
	} else if pod.Status.Phase == corev1.PodPending {
		return "pending"
	} else if pod.Status.Phase == corev1.PodRunning {
		return "running"
	}

	return ""
}

func prepareArchive(requestId string) (archivePath string, err error) {
	log.Printf("preparing archive for %s", requestId)

	requestOutputDir := path.Join(requestsBaseDir, requestId)
	resultsDir := path.Join(requestOutputDir, "results")
	archivePath = path.Join(requestOutputDir, fmt.Sprintf("%s.tar.gz", requestId))

	return archivePath, targz.Compress(resultsDir, archivePath)
}
