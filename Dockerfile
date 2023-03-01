FROM golang:1.20 as builder
WORKDIR /go/src/github.com/AutomatedProcessImprovement/simod-job-controller-go
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /go/bin/simod-job-controller-go .

FROM scratch
COPY --from=builder /go/bin/simod-job-controller-go /usr/bin/simod-job-controller-go

ENV BROKER_URL=amqp://guest:guest@rabbitmq-service:5672
ENV SIMOD_EXCHANGE_NAME=simod
ENV SIMOD_DOCKER_IMAGE=nokal/simod:3.3.0
ENV KUBERNETES_NAMESPACE=default

CMD ["/usr/bin/simod-job-controller-go", "run"]
