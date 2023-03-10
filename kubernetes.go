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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	clientBatchV1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	clientCoreV1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func submitKubernetesJob(requestId string) (jobName string, err error) {
	log.Printf("submitting job for %s", requestId)

	jobsClient, err := setupAndMakeJobsClient()
	if err != nil {
		return "", fmt.Errorf("failed to setup jobs client: %s", err)
	}

	requestOutputDir := path.Join(requestsBaseDir, requestId)
	configPath := path.Join(requestOutputDir, configurationFileName)
	resultsOutputDir := path.Join(requestOutputDir, "results")
	err = os.MkdirAll(resultsOutputDir, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create results output dir at %s: %s", resultsOutputDir, err)
	}

	job := makeJobForRequest(requestId, configPath, resultsOutputDir)
	if job == nil {
		return "", fmt.Errorf("failed to create job for %s", requestId)
	}

	ctx := context.Background()
	_, err = jobsClient.Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create job for %s: %s", requestId, err)
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
	ttlSeconds := int32(60 * 3)

	jobName := jobNameFromRequestId(requestId)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: kubernetesNamespace,
			Labels: map[string]string{
				"app": "simod-job",
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
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(simodJobResourceCpuLimit),
									corev1.ResourceMemory: resource.MustParse(simodJobResourceMemoryLimit),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(simodJobResourceCpuRequest),
									corev1.ResourceMemory: resource.MustParse(simodJobResourceMemoryRequest),
								},
							},
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

func watchPodsAndPrepareArchive(job *Job) {
	previousJobStatus := job.State()
	log.Printf("starting pods watcher for job %s with status %s", job.Name, previousJobStatus)

	podsClient, err := setupAndMakePodsClient()
	if err != nil {
		log.Printf("failed to setup pods client: %s", err)
		job.SetFailedWithoutErr()
		return
	}

	podsWatcher, err := podsClient.Watch(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", job.Name),
		Watch:         true,
	})
	if err != nil {
		log.Printf("failed to watch pods for job %s: %s", job.Name, err)
		job.SetFailedWithoutErr()
		return
	}

	defer func() {
		podsWatcher.Stop()
		log.Printf("pods watcher for job %s stopped", job.Name)
	}()

	podsWatcherChan := podsWatcher.ResultChan()

	for event := range podsWatcherChan {
		switch event.Type {
		case watch.Error:
			log.Printf("error watching pods for job %s: %s", job.Name, event.Object)
			job.SetFailedWithoutErr()
			return
		case watch.Deleted:
			log.Printf("pod for job %s was deleted", job.Name)
			job.SetSucceededWithoutErr()
			return
		case watch.Added, watch.Modified:
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				log.Printf("failed to cast event object to pod")
				job.SetFailedWithoutErr()
				return
			}

			podStatus := parsePodStatus(pod)
			log.Printf("pod %s for job %s is %s", pod.Name, job.Name, podStatus)

			if podStatus == "" {
				continue
			}

			switch podStatus {
			case "", "pending":
				continue
			case "running":
				job.SetRunningWithoutErr()
			case "succeeded":
				if _, err := prepareArchive(job.RequestID); err != nil {
					log.Printf("failed to prepare archive for %s", job.RequestID)
					job.SetFailedWithoutErr()
				} else {
					job.SetSucceededWithoutErr()
				}
				return
			case "failed":
				job.SetFailedWithoutErr()
				return
			default:
				log.Printf("unhandled pod's status for job %s: %s", job.Name, podStatus)
			}
		default:
			log.Printf("unhandled pod's event type for job %s: %s", job.Name, event.Type)
		}
	}
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
