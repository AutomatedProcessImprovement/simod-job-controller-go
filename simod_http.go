package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

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

	request, err := http.NewRequest("PATCH", fmt.Sprintf("%s/discoveries/%s", simodURL, requestId), bytes.NewBuffer(payloadBytes))
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
