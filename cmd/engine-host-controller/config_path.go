package main

import (
	"os"
	"path/filepath"
)

func defaultConfigPath() string {
	if configured := os.Getenv("WEKNORA_ENGINE_CONTROLLER_CONFIG"); configured != "" {
		return configured
	}
	if programData := os.Getenv("ProgramData"); programData != "" {
		return filepath.Join(programData, "WeKnora", "engine-controller", "config.yaml")
	}
	return filepath.Join(string(filepath.Separator), "etc", "weknora", "engine-controller.yaml")
}
