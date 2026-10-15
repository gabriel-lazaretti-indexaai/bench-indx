package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	streamName      = "benchmark:jobs"
	consumerGroup   = "benchmark-workers"
	jobConcurrency  = 8
	itemConcurrency = 8
	pageSize        = 100
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := envOr("DATABASE_URL", "postgres://benchmark:benchmark@postgres:5432/benchmark?sslmode=disable")
	redisURL := envOr("REDIS_URL", "redis://redis:6379/0")

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		logger.Error("parse database url", "error", err)
		os.Exit(1)
	}
	poolConfig.MaxConns = 20
	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Error("create database pool", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	options, err := redis.ParseURL(redisURL)
	if err != nil {
		logger.Error("parse redis url", "error", err)
		os.Exit(1)
	}
	redisClient := redis.NewClient(options)
	defer redisClient.Close()

	service := &service{db: db, redis: redisClient, logger: logger}
	go service.dispatchLoop(ctx)
	for worker := 0; worker < jobConcurrency; worker++ {
		go service.workerLoop(ctx, fmt.Sprintf("%s-%d", hostname(), worker+1))
	}

	server := &http.Server{
		Addr:              envOr("HTTP_ADDR", ":8080"),
		Handler:           service.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverDone := make(chan error, 1)
	go func() {
		logger.Info("benchmark service listening", "address", server.Addr)
		serverDone <- server.ListenAndServe()
	}()

	select {
	case err := <-serverDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", "error", err)
		}
	}
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "benchmark-worker"
	}
	return name
}
