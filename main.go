package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
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

var (
	version = "0.4.2"

	brokerUrl                     = os.Getenv("BROKER_URL")
	exchangeName                  = os.Getenv("SIMOD_EXCHANGE_NAME")
	simodDockerImage              = os.Getenv("SIMOD_DOCKER_IMAGE")
	simodJobResourceCpuRequest    = os.Getenv("SIMOD_JOB_RESOURCE_CPU_REQUEST")
	simodJobResourceCpuLimit      = os.Getenv("SIMOD_JOB_RESOURCE_CPU_LIMIT")
	simodJobResourceMemoryRequest = os.Getenv("SIMOD_JOB_RESOURCE_MEMORY_REQUEST")
	simodJobResourceMemoryLimit   = os.Getenv("SIMOD_JOB_RESOURCE_MEMORY_LIMIT")
	kubernetesNamespace           = os.Getenv("KUBERNETES_NAMESPACE")

	// requestsBaseDir is the base directory where all requests are stored on the attached volume
	requestsBaseDir = "/tmp/simod-volume/data/requests"

	// configurationFileName is the name of the configuration file that is expected to be present in the request directory
	configurationFileName = "configuration.yaml"

	// prometheusMetrics is the metrics object that is used to expose metrics to Prometheus
	prometheusMetrics *metrics

	// kubernetesClientset is the clientset that is used to interact with the Kubernetes API
	kubernetesClientset *kubernetes.Clientset

	simodUrl = "http://simod-http:8000"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("no arguments provided")
		os.Exit(1)
	}

	cmd := args[0]

	switch cmd {
	case "version":
		fmt.Println(version)
		os.Exit(0)
	case "run":
		validateEnv()
		printEnv()
		log.Println("simod job controller has started")
	default:
		fmt.Printf("unknown command: %s", cmd)
		os.Exit(1)
	}

	run()
}

func validateEnv() {
	if brokerUrl == "" {
		log.Fatal("BROKER_URL is not set")
	}

	if exchangeName == "" {
		log.Fatal("SIMOD_EXCHANGE_NAME is not set")
	}

	if simodDockerImage == "" {
		log.Fatal("SIMOD_DOCKER_IMAGE is not set")
	}

	if kubernetesNamespace == "" {
		log.Fatal("KUBERNETES_NAMESPACE is not set")
	}

	if simodJobResourceCpuRequest == "" {
		log.Fatal("SIMOD_JOB_RESOURCE_CPU_REQUEST is not set")
	}

	if simodJobResourceCpuLimit == "" {
		log.Fatal("SIMOD_JOB_RESOURCE_CPU_LIMIT is not set")
	}

	if simodJobResourceMemoryRequest == "" {
		log.Fatal("SIMOD_JOB_RESOURCE_MEMORY_REQUEST is not set")
	}

	if simodJobResourceMemoryLimit == "" {
		log.Fatal("SIMOD_JOB_RESOURCE_MEMORY_LIMIT is not set")
	}
}

func printEnv() {
	log.Printf("version: %s", version)
	log.Printf("BROKER_URL: %s", brokerUrl)
	log.Printf("SIMOD_EXCHANGE_NAME: %s", exchangeName)
	log.Printf("SIMOD_DOCKER_IMAGE: %s", simodDockerImage)
	log.Printf("SIMOD_JOB_RESOURCE_CPU_REQUEST: %s", simodJobResourceCpuRequest)
	log.Printf("SIMOD_JOB_RESOURCE_CPU_LIMIT: %s", simodJobResourceCpuLimit)
	log.Printf("SIMOD_JOB_RESOURCE_MEMORY_REQUEST: %s", simodJobResourceMemoryRequest)
	log.Printf("SIMOD_JOB_RESOURCE_MEMORY_LIMIT: %s", simodJobResourceMemoryLimit)
	log.Printf("KUBERNETES_NAMESPACE: %s", kubernetesNamespace)
}

func run() {
	conn, err := amqp.Dial(brokerUrl)
	failOnError(err, "failed to connect to RabbitMQ")
	defer conn.Close()

	go func() {
		log.Printf("closing: %s", <-conn.NotifyClose(make(chan *amqp.Error)))
		os.Exit(1)
	}()

	channel, err := conn.Channel()
	failOnError(err, "failed to open a channel")
	defer channel.Close()

	durable := true
	autoDelete := false
	internal := false
	noWait := false
	extraParams := amqp.Table{}
	err = channel.ExchangeDeclare(
		exchangeName,
		"topic",
		durable,
		autoDelete,
		internal,
		noWait,
		extraParams,
	)
	failOnError(err, "failed to declare a queue")

	exclusive := true
	pendingQueue, err := channel.QueueDeclare(
		"",
		durable,
		autoDelete,
		exclusive,
		noWait,
		extraParams,
	)
	failOnError(err, "failed to declare a queue")

	err = channel.QueueBind(
		pendingQueue.Name,
		"requests.status.pending",
		exchangeName,
		noWait,
		extraParams,
	)
	failOnError(err, "failed to bind a queue")

	consumerName := ""
	autoAck := false
	noLocal := false
	pendingMsgs, err := channel.Consume(
		pendingQueue.Name,
		consumerName,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		extraParams,
	)
	failOnError(err, "failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range pendingMsgs {
			go handleDelivery(d, channel)
		}
	}()

	setupMetricsAndServe()

	log.Printf("waiting for messages")
	<-forever
}

func setupMetricsAndServe() {
	registry := prometheus.NewRegistry()
	prometheusMetrics = newMetrics(registry)

	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
		http.ListenAndServe(":8080", nil)
	}()
}

func handleDelivery(d amqp.Delivery, brokerChannel *amqp.Channel) {
	requestId := string(d.Body)
	log.Printf("received %s", requestId)

	currentStatus := extractStatus(d.RoutingKey)
	prometheusMetrics.addNewJob(currentStatus, requestId)

	jobName, err := submitJob(requestId)
	d.Ack(false)

	if err != nil {
		log.Printf("failed to submit job for %s: %s", requestId, err)
		newStatus := "failed"
		updateJobStatusHttp(requestId, newStatus)
	} else {
		watchPodsAndPrepareArchive(jobName, requestId, currentStatus, brokerChannel)
	}
}

func extractStatus(routingKey string) string {
	parts := strings.Split(routingKey, ".")
	return parts[len(parts)-1]
}

func submitJob(requestId string) (jobName string, err error) {
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

func watchPodsAndPrepareArchive(jobName, requestId, previousJobStatus string, brokerChannel *amqp.Channel) {
	log.Printf("starting pods watcher for job %s", jobName)

	podsClient, err := setupAndMakePodsClient()
	if err != nil {
		log.Printf("failed to setup pods client: %s", err)
		return
	}

	podsWatcher, err := podsClient.Watch(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
		Watch:         true,
	})
	if err != nil {
		log.Printf("failed to watch pods for job %s: %s", jobName, err)
		return
	}

	podsWatcherChan := podsWatcher.ResultChan()

	go func() {
		for event := range podsWatcherChan {
			switch event.Type {
			case watch.Error:
				log.Printf("error watching pods for job %s: %s", jobName, event.Object)
				podsWatcher.Stop()
				return
			case watch.Deleted:
				log.Printf("pod for job %s was deleted", jobName)
				podsWatcher.Stop()
				return
			case watch.Added, watch.Modified:
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					log.Printf("failed to cast event object to pod")
					podsWatcher.Stop()
					return
				}

				podStatus := parsePodStatus(pod)
				log.Printf("pod %s for job %s is %s", pod.Name, jobName, podStatus)

				if podStatus == "" {
					continue
				}

				if podStatus != previousJobStatus {
					updateJobStatusHttp(requestId, podStatus)
					prometheusMetrics.updateJob(previousJobStatus, podStatus, requestId)
					previousJobStatus = podStatus
				}

				if podStatus == "succeeded" {
					if _, err := prepareArchive(requestId); err != nil {
						log.Printf("failed to prepare archive for %s", requestId)
						updateJobStatusHttp(requestId, "failed")
					}
					podsWatcher.Stop()
					return
				} else if podStatus == "failed" {
					podsWatcher.Stop()
					return
				}
			default:
				log.Printf("unhandled pod's event type for job %s: %s", jobName, event.Type)
			}
		}

		log.Printf("pods watcher for job %s stopped", jobName)
	}()
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

type patchJobRequestBody struct {
	Status string `json:"status"`
}

func updateJobStatusHttp(requestId, status string) error {
	payload := patchJobRequestBody{
		Status: status,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %s", err)
	}

	request, err := http.NewRequest("PATCH", fmt.Sprintf("%s/discoveries/%s", simodUrl, requestId), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err)
	}

	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %s", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	return nil
}

func prepareArchive(requestId string) (archivePath string, err error) {
	log.Printf("preparing archive for %s", requestId)

	requestOutputDir := path.Join(requestsBaseDir, requestId)
	resultsDir := path.Join(requestOutputDir, "results")
	archivePath = path.Join(requestOutputDir, fmt.Sprintf("%s.tar.gz", requestId))

	return archivePath, targz.Compress(resultsDir, archivePath)
}

func jobNameFromRequestId(requestId string) string {
	return fmt.Sprintf("simod-%s", requestId)
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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func logOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}
