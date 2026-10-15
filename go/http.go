package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var jobIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type service struct {
	db     *pgxpool.Pool
	redis  *redis.Client
	logger *slog.Logger
}

type createJobRequest struct {
	Workflow          string `json:"workflow"`
	ItemCount         int    `json:"itemCount"`
	ExternalLatencyMs int    `json:"externalLatencyMs"`
	FailureEvery      int    `json:"failureEvery"`
}

type createJobResponse struct {
	JobID  string `json:"jobId"`
	Status string `json:"status"`
}

type jobStatusResponse struct {
	JobID          string     `json:"jobId"`
	Workflow       string     `json:"workflow"`
	Status         string     `json:"status"`
	TotalItems     int        `json:"totalItems"`
	CompletedItems int        `json:"completedItems"`
	FailedItems    int        `json:"failedItems"`
	CreatedAt      time.Time  `json:"createdAt"`
	StartedAt      *time.Time `json:"startedAt"`
	CompletedAt    *time.Time `json:"completedAt"`
	DurationMs     *int64     `json:"durationMs"`
}

func (s *service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":         "ok",
			"runtime":        "go",
			"runtimeVersion": runtime.Version(),
			"serviceVersion": envOr("APP_VERSION", "dev"),
		})
	})
	mux.HandleFunc("POST /api/benchmark/jobs", s.createJob)
	mux.HandleFunc("GET /api/benchmark/jobs/{jobId}", s.getJob)
	return mux
}

func (s *service) createJob(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var request createJobRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request must contain one JSON object")
		return
	}
	if err := request.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	jobID, err := s.createJobTransaction(r.Context(), request)
	if err != nil {
		s.logger.Error("create benchmark job", "error", err)
		writeError(w, http.StatusServiceUnavailable, "benchmark storage is unavailable")
		return
	}
	writeJSON(w, http.StatusAccepted, createJobResponse{JobID: jobID, Status: "queued"})
}

func (r createJobRequest) validate() error {
	switch r.Workflow {
	case "approval", "enrichment", "synchronization":
	default:
		return errors.New("workflow must be approval, enrichment, or synchronization")
	}
	if r.ItemCount < 1 || r.ItemCount > 5000 {
		return errors.New("itemCount must be between 1 and 5000")
	}
	if r.ExternalLatencyMs < 0 || r.ExternalLatencyMs > 200 {
		return errors.New("externalLatencyMs must be between 0 and 200")
	}
	if r.FailureEvery < 0 {
		return errors.New("failureEvery must be greater than or equal to 0")
	}
	return nil
}

func (s *service) getJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobId")
	if !jobIDPattern.MatchString(jobID) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	var result jobStatusResponse
	err := s.db.QueryRow(r.Context(), `
		SELECT id::text, workflow, status, total_items, completed_items, failed_items,
		       created_at, started_at, completed_at,
		       CASE WHEN started_at IS NULL THEN NULL
		            ELSE GREATEST(0, (EXTRACT(EPOCH FROM (COALESCE(completed_at, clock_timestamp()) - started_at)) * 1000)::bigint)
		       END
		FROM benchmark_jobs
		WHERE id = $1::uuid`, jobID).Scan(
		&result.JobID,
		&result.Workflow,
		&result.Status,
		&result.TotalItems,
		&result.CompletedItems,
		&result.FailedItems,
		&result.CreatedAt,
		&result.StartedAt,
		&result.CompletedAt,
		&result.DurationMs,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		s.logger.Error("read benchmark job", "jobId", jobID, "error", err)
		writeError(w, http.StatusServiceUnavailable, "benchmark storage is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
