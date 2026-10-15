package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

var errJobLocked = errors.New("job is already being processed")

type outboxEntry struct {
	id    int64
	jobID string
}

func (s *service) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.dispatchPending(ctx); err != nil && ctx.Err() == nil {
			s.logger.Warn("dispatch benchmark outbox failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *service) dispatchPending(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, job_id::text
		FROM benchmark_outbox
		WHERE dispatched_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, pageSize)
	if err != nil {
		return err
	}
	entries := make([]outboxEntry, 0, pageSize)
	for rows.Next() {
		var entry outboxEntry
		if err := rows.Scan(&entry.id, &entry.jobID); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(entries) == 0 {
		return tx.Commit(ctx)
	}

	for _, entry := range entries {
		if err := s.redis.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			Values: map[string]any{
				"jobId":    entry.jobID,
				"outboxId": strconv.FormatInt(entry.id, 10),
			},
		}).Err(); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE benchmark_outbox SET dispatched_at = clock_timestamp() WHERE id = $1`, entry.id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *service) workerLoop(ctx context.Context, consumer string) {
	for ctx.Err() == nil {
		if err := s.redis.XGroupCreateMkStream(ctx, streamName, consumerGroup, "0").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			s.logger.Warn("create benchmark stream group failed", "consumer", consumer, "error", err)
			if !waitContext(ctx, time.Second) {
				return
			}
			continue
		}
		break
	}

	claimCursor := "0-0"
	for ctx.Err() == nil {
		claimed, nextCursor, err := s.redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   streamName,
			Group:    consumerGroup,
			Consumer: consumer,
			MinIdle:  30 * time.Second,
			Start:    claimCursor,
			Count:    1,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("claim benchmark stream message failed", "consumer", consumer, "error", err)
			if !waitContext(ctx, time.Second) {
				return
			}
			continue
		}
		claimCursor = nextCursor
		if len(claimed) > 0 {
			s.handleStreamMessage(ctx, consumer, claimed[0])
			continue
		}

		streams, err := s.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: consumer,
			Streams:  []string{streamName, ">"},
			Count:    1,
			Block:    2 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			s.logger.Warn("read benchmark stream failed", "consumer", consumer, "error", err)
			if !waitContext(ctx, time.Second) {
				return
			}
			continue
		}
		for _, stream := range streams {
			for _, message := range stream.Messages {
				s.handleStreamMessage(ctx, consumer, message)
			}
		}
	}
}

func (s *service) handleStreamMessage(ctx context.Context, consumer string, message redis.XMessage) {
	jobID, ok := message.Values["jobId"].(string)
	if !ok || !jobIDPattern.MatchString(jobID) {
		s.logger.Error("invalid benchmark stream message", "consumer", consumer, "messageId", message.ID)
		return
	}
	if err := s.processJob(ctx, jobID); err != nil {
		if !errors.Is(err, errJobLocked) && ctx.Err() == nil {
			s.logger.Warn("process benchmark job failed", "jobId", jobID, "consumer", consumer, "error", err)
		}
		return
	}
	if err := s.redis.XAck(ctx, streamName, consumerGroup, message.ID).Err(); err != nil && ctx.Err() == nil {
		s.logger.Warn("acknowledge benchmark stream message failed", "jobId", jobID, "messageId", message.ID, "error", err)
	}
}

func (s *service) processJob(ctx context.Context, jobID string) error {
	connection, err := s.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer connection.Release()

	var locked bool
	if err := connection.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, jobID).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return errJobLocked
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var unlocked bool
		if err := connection.QueryRow(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, jobID).Scan(&unlocked); err != nil {
			s.logger.Error("release benchmark job lock", "jobId", jobID, "error", err)
		}
	}()

	var job benchmarkJob
	err = connection.QueryRow(ctx, `
		SELECT id::text, workflow, status, total_items, external_latency_ms, failure_every
		FROM benchmark_jobs
		WHERE id = $1::uuid`, jobID).Scan(
		&job.ID, &job.Workflow, &job.Status, &job.TotalItems, &job.ExternalLatencyMs, &job.FailureEvery,
	)
	if err != nil {
		return err
	}
	if isTerminal(job.Status) {
		return nil
	}
	if _, err := connection.Exec(ctx, `
		UPDATE benchmark_jobs
		SET status = 'running', started_at = COALESCE(started_at, clock_timestamp())
		WHERE id = $1::uuid`, jobID); err != nil {
		return err
	}
	if _, err := connection.Exec(ctx, `
		UPDATE benchmark_job_items
		SET status = 'pending', updated_at = clock_timestamp()
		WHERE job_id = $1::uuid AND status = 'processing'`, jobID); err != nil {
		return err
	}

	for {
		items, err := s.claimJobPage(ctx, jobID)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			break
		}
		if err := s.processItems(ctx, job, items); err != nil {
			return err
		}
	}

	_, err = connection.Exec(ctx, `
		UPDATE benchmark_jobs
		SET status = CASE WHEN failed_items > 0 THEN 'completed_with_errors' ELSE 'completed' END,
		    completed_at = clock_timestamp()
		WHERE id = $1::uuid AND status = 'running'`, jobID)
	return err
}

type benchmarkJob struct {
	ID                string
	Workflow          string
	Status            string
	TotalItems        int
	ExternalLatencyMs int
	FailureEvery      int
}

type benchmarkItem struct {
	Index         int
	ExternalID    string
	ProductID     *int64
	SourcePayload []byte
}

func (s *service) claimJobPage(ctx context.Context, jobID string) ([]benchmarkItem, error) {
	rows, err := s.db.Query(ctx, `
		SELECT item_index, external_id, product_id, source_payload
		FROM benchmark_job_items
		WHERE job_id = $1::uuid AND status = 'pending'
		ORDER BY item_index
		LIMIT $2`, jobID, pageSize)
	if err != nil {
		return nil, err
	}
	items := make([]benchmarkItem, 0, pageSize)
	for rows.Next() {
		var item benchmarkItem
		if err := rows.Scan(&item.Index, &item.ExternalID, &item.ProductID, &item.SourcePayload); err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	return items, nil
}

func (s *service) processItems(ctx context.Context, job benchmarkJob, items []benchmarkItem) error {
	if job.Workflow == "synchronization" && job.ExternalLatencyMs > 0 {
		if err := waitLatency(ctx, job.ExternalLatencyMs); err != nil {
			return err
		}
	}
	semaphore := make(chan struct{}, itemConcurrency)
	var waitGroup sync.WaitGroup
	var firstError error
	var errorOnce sync.Once
	for _, item := range items {
		item := item
		semaphore <- struct{}{}
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			defer func() { <-semaphore }()
			if err := s.processItem(ctx, job, item); err != nil {
				errorOnce.Do(func() { firstError = err })
			}
		}()
	}
	waitGroup.Wait()
	return firstError
}

func (s *service) processItem(ctx context.Context, job benchmarkJob, item benchmarkItem) error {
	var claimed int
	err := s.db.QueryRow(ctx, `
		UPDATE benchmark_job_items
		SET status = 'processing', updated_at = clock_timestamp()
		WHERE job_id = $1::uuid AND item_index = $2 AND status = 'pending'
		RETURNING item_index`, job.ID, item.Index).Scan(&claimed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}

	if job.Workflow != "synchronization" && job.ExternalLatencyMs > 0 {
		if err := waitLatency(ctx, job.ExternalLatencyMs); err != nil {
			return err
		}
	}
	if job.FailureEvery > 0 && item.Index%job.FailureEvery == 0 {
		return s.failItem(ctx, job.ID, item.Index, "Injected deterministic failure")
	}

	switch job.Workflow {
	case "approval":
		return s.completeApproval(ctx, job, item)
	case "enrichment":
		return s.completeEnrichment(ctx, job, item)
	case "synchronization":
		return s.completeSynchronization(ctx, job, item)
	default:
		return fmt.Errorf("unsupported workflow %q", job.Workflow)
	}
}

func (s *service) failItem(ctx context.Context, jobID string, itemIndex int, message string) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var updated int
	err = tx.QueryRow(ctx, `
		UPDATE benchmark_job_items
		SET status = 'failed', error_message = $3, updated_at = clock_timestamp()
		WHERE job_id = $1::uuid AND item_index = $2 AND status = 'processing'
		RETURNING item_index`, jobID, itemIndex, message).Scan(&updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE benchmark_jobs SET failed_items = failed_items + 1 WHERE id = $1::uuid`, jobID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type approvalResult struct {
	Status     string `json:"status"`
	ExternalID string `json:"externalId"`
}

func (s *service) completeApproval(ctx context.Context, job benchmarkJob, item benchmarkItem) error {
	if item.ProductID == nil {
		return fmt.Errorf("approval item %d has no product", item.Index)
	}
	result, err := json.Marshal(approvalResult{
		Status:     "approved",
		ExternalID: item.ExternalID,
	})
	if err != nil {
		return err
	}
	return s.commitItemSuccess(ctx, job.ID, item, result, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE benchmark_products
			SET approval_status = 'approved', updated_at = clock_timestamp()
			WHERE id = $1`, *item.ProductID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("approval product %q not found", item.ExternalID)
		}
		return nil
	})
}

type enrichmentResult struct {
	Title       string         `json:"title"`
	Slug        string         `json:"slug"`
	Description string         `json:"description"`
	Attributes  map[string]any `json:"attributes"`
}

func (s *service) completeEnrichment(ctx context.Context, job benchmarkJob, item benchmarkItem) error {
	if item.ProductID == nil {
		return fmt.Errorf("enrichment item %d has no product", item.Index)
	}
	title := fmt.Sprintf("Optimized Product %d", item.Index)
	resultValue := enrichmentResult{
		Title:       title,
		Slug:        fmt.Sprintf("optimized-product-%d", item.Index),
		Description: fmt.Sprintf("Enriched description for product %d", item.Index),
		Attributes: map[string]any{
			"benchmark": true,
			"material":  map[bool]string{true: "cotton", false: "linen"}[item.Index%2 == 0],
			"color":     map[bool]string{true: "blue", false: "black"}[item.Index%3 == 0],
		},
	}
	result, err := json.Marshal(resultValue)
	if err != nil {
		return err
	}
	return s.commitItemSuccess(ctx, job.ID, item, result, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE benchmark_products
			SET name = $2, description = $3, attributes = $4::jsonb, enrichment = $4::jsonb,
			    updated_at = clock_timestamp()
			WHERE id = $1`, *item.ProductID, resultValue.Title, resultValue.Description, string(result))
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("enrichment product %q not found", item.ExternalID)
		}
		return nil
	})
}

type normalizedSyncPayload struct {
	ExternalID  string         `json:"externalId"`
	SKU         string         `json:"sku"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Category    string         `json:"category"`
	Attributes  map[string]any `json:"attributes"`
}

func (s *service) completeSynchronization(ctx context.Context, job benchmarkJob, item benchmarkItem) error {
	var payload syncPayload
	if err := json.Unmarshal(item.SourcePayload, &payload); err != nil {
		return fmt.Errorf("decode synchronization source payload for item %d: %w", item.Index, err)
	}
	if payload.ExternalID != item.ExternalID || payload.Attributes == nil {
		return fmt.Errorf("invalid synchronization source payload for item %d", item.Index)
	}
	normalized := normalizedSyncPayload{
		ExternalID:  strings.TrimSpace(payload.ExternalID),
		SKU:         strings.TrimSpace(payload.SKU),
		Name:        strings.TrimSpace(payload.Title),
		Description: strings.TrimSpace(payload.Description),
		Category:    strings.ToLower(strings.TrimSpace(payload.Category)),
		Attributes:  payload.Attributes,
	}
	attributes, err := json.Marshal(normalized.Attributes)
	if err != nil {
		return err
	}
	return s.commitItemSuccessWithProduct(ctx, job.ID, item, func(tx pgx.Tx) ([]byte, int64, error) {
		var productID int64
		err := tx.QueryRow(ctx, `
			INSERT INTO benchmark_products (external_id, sku, name, description, category, attributes)
			VALUES ($1, $2, $3, $4, $5, $6::jsonb)
			ON CONFLICT (external_id) DO UPDATE
			SET sku = EXCLUDED.sku, name = EXCLUDED.name, description = EXCLUDED.description,
			    category = EXCLUDED.category, attributes = EXCLUDED.attributes, updated_at = clock_timestamp()
			RETURNING id`, normalized.ExternalID, normalized.SKU, normalized.Name, normalized.Description, normalized.Category, string(attributes)).Scan(&productID)
		if err != nil {
			return nil, 0, err
		}
		encoded, err := json.Marshal(normalized)
		return encoded, productID, err
	})
}

func (s *service) commitItemSuccess(ctx context.Context, jobID string, item benchmarkItem, result []byte, updateProduct func(pgx.Tx) error) error {
	return s.commitItemSuccessWithProduct(ctx, jobID, item, func(tx pgx.Tx) ([]byte, int64, error) {
		if err := updateProduct(tx); err != nil {
			return nil, 0, err
		}
		return result, 0, nil
	})
}

func (s *service) commitItemSuccessWithProduct(ctx context.Context, jobID string, item benchmarkItem, updateProduct func(pgx.Tx) ([]byte, int64, error)) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, productID, err := updateProduct(tx)
	if err != nil {
		return err
	}
	var updated int
	err = tx.QueryRow(ctx, `
		UPDATE benchmark_job_items
		SET status = 'completed', product_id = COALESCE($3, product_id), result = $4::jsonb,
		    error_message = NULL, updated_at = clock_timestamp()
		WHERE job_id = $1::uuid AND item_index = $2 AND status = 'processing'
		RETURNING item_index`, jobID, item.Index, nullableProductID(productID), string(result)).Scan(&updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE benchmark_jobs SET completed_items = completed_items + 1 WHERE id = $1::uuid`, jobID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func nullableProductID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

func isTerminal(status string) bool {
	return status == "completed" || status == "completed_with_errors" || status == "failed"
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func waitLatency(ctx context.Context, milliseconds int) error {
	timer := time.NewTimer(time.Duration(milliseconds) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
