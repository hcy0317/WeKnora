//go:build windows

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/hostcontroller"
	"golang.org/x/sys/windows/svc"
)

const serviceName = "WeKnoraEngineHostController"

func main() {
	configPath := flag.String("config", defaultConfigPath(), "path to the engine controller YAML")
	initCertificates := flag.String("init-certs", "", "create a non-overwriting controller certificate bundle")
	flag.Parse()
	if *initCertificates != "" {
		if err := bootstrapCertificates(*initCertificates); err != nil {
			log.Fatal(err)
		}
		return
	}
	isService, err := svc.IsWindowsService()
	if err != nil {
		log.Fatal(err)
	}
	if isService {
		if err := svc.Run(serviceName, &windowsService{configPath: *configPath}); err != nil {
			log.Fatal(err)
		}
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runApplication(ctx, *configPath); err != nil {
		log.Fatal(err)
	}
}

type windowsService struct {
	configPath string
}

func (s *windowsService) Execute(
	_ []string,
	requests <-chan svc.ChangeRequest,
	changes chan<- svc.Status,
) (bool, uint32) {
	changes <- svc.Status{State: svc.StartPending}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runApplication(ctx, s.configPath) }()
	changes <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}

	for {
		select {
		case err := <-done:
			if err != nil {
				return false, 1
			}
			return false, 0
		case request := <-requests:
			switch request.Cmd {
			case svc.Interrogate:
				changes <- request.CurrentStatus
			case svc.Stop, svc.Shutdown:
				changes <- svc.Status{State: svc.StopPending}
				cancel()
				select {
				case err := <-done:
					if err != nil {
						return false, 1
					}
					return false, 0
				case <-time.After(35 * time.Second):
					return false, 1
				}
			}
		}
	}
}

func runApplication(ctx context.Context, configPath string) error {
	application, err := hostcontroller.NewApplication(configPath)
	if err != nil {
		return fmt.Errorf("initialize engine host controller: %w", err)
	}
	return application.Run(ctx)
}
