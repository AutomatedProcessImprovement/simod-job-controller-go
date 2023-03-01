package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	"k8s.io/client-go/rest"
)

var (
	version             = "0.1.0"
	brokerUrl           = os.Getenv("BROKER_URL")
	exchangeName        = os.Getenv("SIMOD_EXCHANGE_NAME")
	simodDockerImage    = os.Getenv("SIMOD_DOCKER_IMAGE")
	kubernetesNamespace = os.Getenv("KUBERNETES_NAMESPACE")
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
}

func printEnv() {
	log.Printf("BROKER_URL: %s", brokerUrl)
	log.Printf("SIMOD_EXCHANGE_NAME: %s", exchangeName)
	log.Printf("SIMOD_DOCKER_IMAGE: %s", simodDockerImage)
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

	log.Printf("[*] Waiting for messages. To exit press CTRL+C")
	<-forever
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

func handleDelivery(d amqp.Delivery, brokerChannel *amqp.Channel) {
	requestId := string(d.Body)
	routingKey := d.RoutingKey

	log.Printf("Received %s with status %s", requestId, routingKey)

	jobName, err := submitJob(requestId)
	d.Ack(false)

	if err != nil {
		log.Printf("Failed to submit job for %s: %s", requestId, err)
		publishJobStatus(requestId, "failed", brokerChannel)
	} else {
		watchJob(jobName, requestId, brokerChannel)
	}
}

func submitJob(requestId string) (jobName string, err error) {
	log.Printf("Submitting job for %s", requestId)

	jobsClient, err := setupAndMakeJobsClient()
	if err != nil {
		return "", fmt.Errorf("failed to setup jobs client: %s", err)
	}

	backoffLimit := int32(0)
	ttlSeconds := int32(60 * 3)

	jobName = jobNameFromRequestId(requestId)
	requestOutputDir := fmt.Sprintf("/tmp/simod-volume/data/requests/%s", requestId)
	configPath := path.Join(requestOutputDir, "configuration.yaml")
	resultsOutputDir := path.Join(requestOutputDir, "results")

	err = os.MkdirAll(resultsOutputDir, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create results output dir at %s: %s", resultsOutputDir, err)
	}

	job := makeJob(jobName, backoffLimit, ttlSeconds, configPath, resultsOutputDir)

	ctx := context.Background()
	_, err = jobsClient.Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create job for %s: %s", requestId, err)
	}

	return jobName, err
}

func watchJob(jobName, requestId string, brokerChannel *amqp.Channel) {
	log.Printf("Watching job %s for %s", jobName, requestId)

	jobsClient, err := setupAndMakeJobsClient()
	logOnError(err, "failed to setup jobs client")

	ctx := context.Background()

	var previousStatus string
	var delaySeconds int64 = 10

	for {
		job, err := jobsClient.Get(ctx, jobName, metav1.GetOptions{})
		logOnError(err, "failed to get job")

		status := getJobStatus(job)

		if status != previousStatus {
			log.Printf("Job %s for %s is %s", jobName, requestId, status)
			previousStatus = status

			publishJobStatus(requestId, status, brokerChannel)
		}

		if status == "succeeded" || status == "failed" {
			break
		}

		time.Sleep(time.Duration(delaySeconds) * time.Second)
	}

	log.Printf("Finished watching job %s for %s", jobName, requestId)
}

func setupAndMakeJobsClient() (v1.JobInterface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubernetes config: %s", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %s", err)
	}

	jobsClient := clientset.BatchV1().Jobs(kubernetesNamespace)

	return jobsClient, nil
}

func getJobStatus(job *batchv1.Job) string {
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
		return ""
	}
}

func jobNameFromRequestId(requestId string) string {
	return fmt.Sprintf("simod-%s", requestId)
}

func makeJob(jobName string, backoffLimit int32, ttlSeconds int32, configPath string, resultsOutputDir string) *batchv1.Job {
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
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
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
