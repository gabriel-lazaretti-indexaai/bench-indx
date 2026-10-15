package main

import (
	"context"
)

func (s *service) createJobTransaction(ctx context.Context, request createJobRequest) (string, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var jobID string
	err = tx.QueryRow(ctx, `
		INSERT INTO benchmark_jobs (workflow, status, total_items, external_latency_ms, failure_every)
		VALUES ($1, 'queued', $2, $3, $4)
		RETURNING id::text`, request.Workflow, request.ItemCount, request.ExternalLatencyMs, request.FailureEvery).Scan(&jobID)
	if err != nil {
		return "", err
	}

	if request.Workflow == "synchronization" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO benchmark_job_items (job_id, item_index, external_id, source_payload)
			SELECT
				$1::uuid,
				item.item_index,
				$1::text || ':' || item.item_index::text,
				jsonb_build_object(
					'externalId', $1::text || ':' || item.item_index::text,
					'sku', 'SKU-' || item.item_index::text,
					'title', 'Imported Product ' || item.item_index::text,
					'description', 'Imported description for product ' || item.item_index::text,
					'category', 'category-' || (((item.item_index - 1) % 10) + 1)::text,
					'attributes', jsonb_build_object(
						'source', 'fixture',
						'page', ceil(item.item_index::numeric / 100)::integer
					)
				)
			FROM generate_series(1, $2::integer) AS item(item_index)`, jobID, request.ItemCount); err != nil {
			return "", err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			INSERT INTO benchmark_products (external_id, sku, name, description, category)
			SELECT
				$1::text || ':' || item.item_index::text,
				'SKU-' || item.item_index::text,
				'Product ' || item.item_index::text,
				'Synthetic description for product ' || item.item_index::text,
				'category-' || (((item.item_index - 1) % 10) + 1)::text
			FROM generate_series(1, $2::integer) AS item(item_index)`, jobID, request.ItemCount); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO benchmark_job_items (job_id, item_index, external_id, product_id)
			SELECT $1::uuid, item.item_index, product.external_id, product.id
			FROM generate_series(1, $2::integer) AS item(item_index)
			JOIN benchmark_products AS product
			  ON product.external_id = $1::text || ':' || item.item_index::text`, jobID, request.ItemCount); err != nil {
			return "", err
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO benchmark_outbox (job_id, payload)
		VALUES ($1::uuid, jsonb_build_object('jobId', $1::text))`, jobID); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return jobID, nil
}

type syncPayload struct {
	ExternalID  string         `json:"externalId"`
	SKU         string         `json:"sku"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Category    string         `json:"category"`
	Attributes  map[string]any `json:"attributes"`
}
