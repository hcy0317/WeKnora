package main

import (
	"encoding/json"
	"os"

	"github.com/Tencent/WeKnora/internal/engine/hostcontroller"
)

func bootstrapCertificates(root string) error {
	bundle, err := hostcontroller.BootstrapCertificateBundle(root)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(bundle)
}
