package main

import (
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	jobsGauges    *prometheus.GaugeVec
	jobsDurations *prometheus.HistogramVec

	jobsDurationsMap map[string][]*timeRecord

	mx sync.Mutex
}

type timeRecord struct {
	status string
	start  *time.Time
	end    *time.Time
}

func (r *timeRecord) Duration() time.Duration {
	return r.end.Sub(*r.start)
}

func newMetrics(registry prometheus.Registerer) *metrics {
	m := &metrics{
		jobsGauges: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "simod_jobs_gauge",
				Help: "Number of jobs in a given state",
			},
			[]string{"status", "request_id"},
		),
		jobsDurations: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "simod_jobs_duration_seconds",
				Help: "Duration of jobs in seconds",
			},
			[]string{"status", "request_id"},
		),
		jobsDurationsMap: make(map[string][]*timeRecord),
	}

	registry.MustRegister(m.jobsGauges)
	registry.MustRegister(m.jobsDurations)

	return m
}

func (m *metrics) AddNewJob(status, requestId string) {
	m.mx.Lock()
	defer m.mx.Unlock()

	m.jobsGauges.WithLabelValues(status, requestId).Inc()
	m.jobsDurationsMap[requestId] = make([]*timeRecord, 0)
	
	now := time.Now()
	m.jobsDurationsMap[requestId] = append(m.jobsDurationsMap[requestId], &timeRecord{
		status: status,
		start:  &now,
		end:    nil,
	})
}

func (m *metrics) UpdateJob(previousStatus, newStatus, requestId string) {
	m.mx.Lock()
	defer m.mx.Unlock()

	if previousStatus == newStatus {
		log.Printf("new status should be different from the previous one for request %s, %s == %s", requestId, previousStatus, newStatus)
		return
	}

	m.jobsGauges.WithLabelValues(previousStatus, requestId).Dec()
	m.jobsGauges.WithLabelValues(newStatus, requestId).Inc()

	now := time.Now()
	records, ok := m.jobsDurationsMap[requestId]

	if !ok {
		m.jobsDurationsMap[requestId] = []*timeRecord{}
	}

	for _, record := range records {
		if record.status == previousStatus {
			record.end = &now
			m.jobsDurations.WithLabelValues(previousStatus, requestId).Observe(record.Duration().Seconds())
			break
		}
	}

	if !isJobStatusTerminal(newStatus) {
		records = append(records, &timeRecord{
			status: newStatus,
			start:  &now,
			end:    nil,
		})
		m.jobsDurationsMap[requestId] = records
	}
}

func isJobStatusTerminal(status string) bool {
	return status == "succeeded" || status == "failed"
}
