#!/usr/bin/env bash

docker build -t nokal/simod-job-controller-go:$(go run . version) .