//go:build !windows

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tencent/WeKnora/internal/engine/hostcontroller"
)

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
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	application, err := hostcontroller.NewApplication(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := application.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
