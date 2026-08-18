package hostcontroller

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

type Application struct {
	config      lifecycle.Config
	coordinator *lifecycle.Coordinator
	httpServer  *http.Server
	tlsConfig   *tls.Config
}

func NewApplication(configPath string) (*Application, error) {
	configStore := lifecycle.NewConfigStore(configPath)
	config, err := configStore.Load()
	if err != nil {
		return nil, err
	}
	if config.Catalog.DockerHost == "" {
		return nil, errors.New("controller catalog is required")
	}
	if config.Controller.ListenAddress == "" {
		return nil, errors.New("controller settings are required")
	}
	dockerCLI, err := NewDockerCLI(config.Controller.DockerExecutable, config.Catalog.DockerHost)
	if err != nil {
		return nil, err
	}
	dockerRuntime, err := NewDockerRuntime(
		config.Catalog,
		dockerCLI,
		WithObserveOnly(config.Controller.ObserveOnly),
	)
	if err != nil {
		return nil, err
	}
	coordinator, err := lifecycle.NewCoordinator(*config, dockerRuntime)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := LoadMutualTLSConfig(config.Controller.TLS)
	if err != nil {
		return nil, err
	}
	api := NewAPI(coordinator, configStore)
	return &Application{
		config:      *config,
		coordinator: coordinator,
		tlsConfig:   tlsConfig,
		httpServer: &http.Server{
			Addr:              config.Controller.ListenAddress,
			Handler:           api,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      130 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}, nil
}

func (a *Application) Run(ctx context.Context) error {
	var ownership Ownership
	var err error
	if !a.config.Controller.ObserveOnly {
		ownership, err = AcquireOwnership(a.config.Controller.OwnerMutex)
		if err != nil {
			return err
		}
		defer ownership.Close()
	}

	listener, err := net.Listen("tcp", a.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listen for engine controller: %w", err)
	}
	defer listener.Close()
	tlsListener := tls.NewListener(listener, a.tlsConfig)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- a.httpServer.Serve(tlsListener)
	}()

	if err := a.coordinator.EnsureAlwaysOn(ctx); err != nil {
		_ = a.httpServer.Close()
		return err
	}

	var sweep <-chan time.Time
	var ticker *time.Ticker
	if !a.config.Controller.ObserveOnly {
		ticker = time.NewTicker(time.Duration(a.config.Controller.SweepIntervalSeconds) * time.Second)
		defer ticker.Stop()
		sweep = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := a.httpServer.Shutdown(shutdownContext); err != nil {
				return err
			}
			return nil
		case err := <-serverDone:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return fmt.Errorf("serve engine controller: %w", err)
		case <-sweep:
			if err := a.coordinator.EnsureAlwaysOn(ctx); err != nil {
				return err
			}
			if err := a.coordinator.SweepIdle(ctx); err != nil {
				return err
			}
		}
	}
}
