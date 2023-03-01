FROM golang:1.20 as builder
WORKDIR /go/src/github.com/AutomatedProcessImprovement/simod-job-controller-go
COPY . .
RUN go build -ldflags "-X main.version=0.1.0" -o /go/bin/simod-job-controller-go .

FROM scratch
COPY --from=builder /go/bin/simod-job-controller-go /usr/bin/simod-job-controller-go
ENTRYPOINT ["simod-job-controller-go", "run"]
