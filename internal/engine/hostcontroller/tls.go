package hostcontroller

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

func NewMutualTLSConfig(serverCertificate tls.Certificate, clientCAs *x509.CertPool) (*tls.Config, error) {
	if len(serverCertificate.Certificate) == 0 {
		return nil, errors.New("controller server certificate is required")
	}
	if clientCAs == nil {
		return nil, errors.New("controller client CA pool is required")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		NextProtos:   []string{"h2", "http/1.1"},
	}, nil
}

func LoadMutualTLSConfig(files lifecycle.TLSFilesConfig) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(files.Certificate, files.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("load controller server key pair: %w", err)
	}
	caPEM, err := os.ReadFile(files.ClientCA)
	if err != nil {
		return nil, fmt.Errorf("read controller client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("controller client CA file contains no certificates")
	}
	return NewMutualTLSConfig(certificate, clientCAs)
}
