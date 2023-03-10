package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"k8s.io/client-go/kubernetes"
)

var (
	version = "0.5.0"

	brokerUrl                     = os.Getenv("BROKER_URL")
	exchangeName                  = os.Getenv("SIMOD_EXCHANGE_NAME")
	simodDockerImage              = os.Getenv("SIMOD_DOCKER_IMAGE")
	simodJobResourceCpuRequest    = os.Getenv("SIMOD_JOB_RESOURCE_CPU_REQUEST")
	simodJobResourceCpuLimit      = os.Getenv("SIMOD_JOB_RESOURCE_CPU_LIMIT")
	simodJobResourceMemoryRequest = os.Getenv("SIMOD_JOB_RESOURCE_MEMORY_REQUEST")
	simodJobResourceMemoryLimit   = os.Getenv("SIMOD_JOB_RESOURCE_MEMORY_LIMIT")
	simodHttpHost                 = os.Getenv("SIMOD_HTTP_HOST")
	simodHttpPort                 = os.Getenv("SIMOD_HTTP_PORT")
	kubernetesNamespace           = os.Getenv("KUBERNETES_NAMESPACE")

	// requestsBaseDir is the base directory where all requests are stored on the attached volume
	requestsBaseDir = "/tmp/simod-volume/data/requests"

	// configurationFileName is the name of the configuration file that is expected to be present in the request directory
	configurationFileName = "configuration.yaml"

	// prometheusMetrics is the metrics object that is used to expose metrics to Prometheus
	prometheusMetrics *metrics

	// kubernetesClientset is the clientset that is used to interact with the Kubernetes API
	kubernetesClientset *kubernetes.Clientset

	simodUrl = fmt.Sprintf("http://%s:%s", simodHttpHost, simodHttpPort)

	watcherController = newWatcherController()
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
	prometheusMetrics = NewMetrics(registry)

	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
		http.ListenAndServe(":8080", nil)
	}()
}

func handleDelivery(d amqp.Delivery, brokerChannel *amqp.Channel) {
	defer d.Ack(false)

	requestId := string(d.Body)
	currentStatus := extractStatus(d.RoutingKey)

	job := NewJob(requestId, currentStatus, prometheusMetrics, brokerChannel)

	_, err := job.SubmitToKubernetes()
	if err != nil {
		log.Printf("failed to submit job for %s: %s", requestId, err)
		if err = job.SetFailed(); err != nil {
			log.Printf("failed to fail job for %s: %s", requestId, err)
		}
		return
	}

	job.Watch()
}

func extractStatus(routingKey string) string {
	parts := strings.Split(routingKey, ".")
	return parts[len(parts)-1]
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
