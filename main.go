package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

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
	version = "0.3.0"

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
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("No arguments provided")
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
		log.Println("Simod job controller has started")
	default:
		fmt.Printf("Unknown command: %s", cmd)
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
	log.Printf("Version: %s", version)
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

	log.Printf("Waiting for messages")
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
	log.Printf("Received %s", requestId)

	currentStatus := extractStatus(d.RoutingKey)
	prometheusMetrics.addNewJob(currentStatus, requestId)

	jobName, err := submitJob(requestId)
	d.Ack(false)

	if err != nil {
		log.Printf("Failed to submit job for %s: %s", requestId, err)
		newStatus := "failed"
		publishJobStatus(requestId, newStatus, brokerChannel)
	} else {
		// watchJobAndPrepareArchive(jobName, requestId, currentStatus, brokerChannel)
		watchJobAndPodsAndPrepareArchive(jobName, requestId, currentStatus, brokerChannel)
	}
}

func extractStatus(routingKey string) string {
	parts := strings.Split(routingKey, ".")
	return parts[len(parts)-1]
}

func submitJob(requestId string) (jobName string, err error) {
	log.Printf("Submitting job for %s", requestId)

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

func watchJobAndPrepareArchive(jobName, requestId, previousStatus string, brokerChannel *amqp.Channel) {
	log.Printf("Watching job %s", jobName)

	jobsClient, err := setupAndMakeJobsClient()
	logOnError(err, "failed to setup jobs client")

	var delaySeconds int64 = 10

	for {
		ctx := context.Background()
		job, err := jobsClient.Get(ctx, jobName, metav1.GetOptions{})
		logOnError(err, "failed to get job")

		status := parseJobStatus(job)
		if status == "" {
			continue
		}

		if status != previousStatus {
			log.Printf("Job %s is %s", jobName, status)

			publishJobStatus(requestId, status, brokerChannel)
			prometheusMetrics.updateJob(previousStatus, status, requestId)

			previousStatus = status
		}

		if status == "succeeded" {
			_, err = prepareArchive(requestId)
			if err != nil {
				log.Printf("failed to prepare archive for %s", requestId)
				publishJobStatus(requestId, "failed", brokerChannel)
			}
			break
		} else if status == "failed" {
			break
		}

		time.Sleep(time.Duration(delaySeconds) * time.Second)
	}

	log.Printf("Finished watching job %s", jobName)
}

func watchJobAndPodsAndPrepareArchive(jobName, requestId, previousJobStatus string, brokerChannel *amqp.Channel) {
	log.Printf("Watching job %s", jobName)

	jobsClient, err := setupAndMakeJobsClient()
	if err != nil {
		log.Printf("failed to setup jobs client: %s", err)
		return
	}

	jobWatcher, err := jobsClient.Watch(context.Background(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", jobName),
		Watch:         true,
	})
	if err != nil {
		log.Printf("failed to watch job %s: %s", jobName, err)
		return
	}

	jobWatcherChan := jobWatcher.ResultChan()

	go func() {
		for event := range jobWatcherChan {
			switch event.Type {
			case watch.Error:
				log.Printf("error watching job %s: %s", jobName, event.Object)
				jobWatcher.Stop()
				return
			case watch.Deleted:
				log.Printf("job %s was deleted", jobName)
				jobWatcher.Stop()
				return
			case watch.Added, watch.Modified:
				job, ok := event.Object.(*batchv1.Job)
				if !ok {
					log.Printf("failed to cast event object to job")
					jobWatcher.Stop()
					return
				}

				jobStatus := parseJobStatus(job)
				log.Printf("job %s is %s", jobName, jobStatus)

				if jobStatus == "" {
					continue
				}

				// if jobStatus != previousJobStatus {
				// 	publishJobStatus(requestId, jobStatus, brokerChannel)
				// 	prometheusMetrics.updateJob(previousJobStatus, jobStatus, requestId)
				// 	previousJobStatus = jobStatus
				// }

				if jobStatus == "succeeded" {
					// _, err := prepareArchive(requestId)
					// if err != nil {
					// log.Printf("failed to prepare archive for %s", requestId)
					// publishJobStatus(requestId, "failed", brokerChannel)
					// }
					jobWatcher.Stop()
					return
				} else if jobStatus == "failed" {
					jobWatcher.Stop()
					return
				}
			default:
				log.Printf("unhandled event type for job %s: %s", jobName, event.Type)
			}
		}
	}()

	// watch job pods

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
				log.Printf("pod %s is %s", pod.Name, podStatus)

				if podStatus == "" {
					continue
				}

				if podStatus != previousJobStatus {
					publishJobStatus(requestId, podStatus, brokerChannel)
					prometheusMetrics.updateJob(previousJobStatus, podStatus, requestId)
					previousJobStatus = podStatus
				}

				if podStatus == "succeeded" {
					_, err := prepareArchive(requestId)
					if err != nil {
						log.Printf("failed to prepare archive for %s", requestId)
						publishJobStatus(requestId, "failed", brokerChannel)
					}
					podsWatcher.Stop()
					return
				} else if podStatus == "failed" {
					podsWatcher.Stop()
					return
				}
			default:
				log.Printf("unhandled event type for pod for job %s: %s", jobName, event.Type)
			}
		}
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

func parseJobStatus(job *batchv1.Job) string {
	if job == nil {
		return ""
	}

	if job.Status.Succeeded == 1 {
		return "succeeded"
	} else if job.Status.Failed == 1 {
		return "failed"
	} else if job.Status.Active == 1 {
		return "running"
	} else {
		log.Printf("Unknown status for %s: %s", job.Name, job.Status.String())
		return ""
	}
}

func publishJobStatus(requestId, status string, brokerChannel *amqp.Channel) {
	log.Printf("Publishing status %s for %s", status, requestId)

	err := brokerChannel.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare an exchange")

	routingKey := fmt.Sprintf("requests.status.%s", status)

	ctx := context.Background()
	err = brokerChannel.PublishWithContext(
		ctx,
		exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(requestId),
		},
	)
	failOnError(err, "Failed to publish a message")
}

func prepareArchive(requestId string) (archivePath string, err error) {
	log.Printf("Preparing archive for %s", requestId)

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
