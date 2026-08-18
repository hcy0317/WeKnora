//go:build !windows

package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Tencent/WeKnora/internal/engine/hostcontroller"
)

func main() {
	configPath := flag.String("config", defaultConfigPath(), "path to the engine controller YAML")
	initCertificates := flag.String("init-certs", "", "create a non-overwriting controller certificate bundle")
	setObserveOnly := flag.String("set-observe-only", "", "atomically set controller observe-only mode (true or false)")
	flag.Parse()
	if *initCertificates != "" {
		if err := bootstrapCertificates(*initCertificates); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *setObserveOnly != "" {
		value, parseErr := strconv.ParseBool(*setObserveOnly)
		if parseErr != nil {
			log.Fatal("set-observe-only must be true or false")
		}
		updated, updateErr := setControllerObserveOnly(*configPath, value)
		if updateErr != nil {
			log.Fatal(updateErr)
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"observe_only": updated.Controller.ObserveOnly,
			"revision":     updated.Revision,
		})
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
