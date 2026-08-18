package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/gateway"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "check the local gateway health endpoint")
	flag.Parse()
	if *healthcheck {
		client := &http.Client{Timeout: 3 * time.Second}
		response, err := client.Get("http://127.0.0.1:18084/healthz")
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			log.Fatalf("gateway health returned %s", response.Status)
		}
		return
	}
	controllerClient, err := gateway.NewControllerClient(gateway.ControllerClientConfig{
		BaseURL:        envOr("ENGINE_CONTROLLER_URL", "https://host.docker.internal:18443"),
		CACertPath:     envOr("ENGINE_CONTROLLER_CA", "/run/weknora-engine-tls/ca.crt"),
		CertPath:       envOr("ENGINE_CONTROLLER_CERT", "/run/weknora-engine-tls/client.crt"),
		KeyPath:        envOr("ENGINE_CONTROLLER_KEY", "/run/weknora-engine-tls/client.key"),
		GatewayID:      envOr("ENGINE_GATEWAY_ID", "engine-gateway"),
		RequestTimeout: durationOr("ENGINE_CONTROLLER_TIMEOUT", 130*time.Second),
	})
	if err != nil {
		log.Fatal(err)
	}
	handler, err := gateway.New(gateway.Config{
		LeaseClient:     controllerClient,
		GatewayID:       envOr("ENGINE_GATEWAY_ID", "engine-gateway"),
		Routes:          gateway.DefaultRoutes(),
		AllowedBackends: gateway.DefaultAllowedBackends(),
		RenewInterval:   durationOr("ENGINE_LEASE_RENEW_INTERVAL", 10*time.Second),
	})
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{
		Addr:              envOr("ENGINE_GATEWAY_LISTEN", ":18084"),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			log.Fatal(err)
		}
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func durationOr(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		log.Fatalf("invalid %s duration %q", name, value)
	}
	return duration
}
