package hostcontroller

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

type CertificateBundle struct {
	Root                 string
	CACertificate        string
	ServerCertificate    string
	ServerPrivateKey     string
	GatewayCertificate   string
	GatewayPrivateKey    string
	BackendCertificate   string
	BackendPrivateKey    string
	BootstrapCertificate string
	BootstrapPrivateKey  string
}

func BootstrapCertificateBundle(root string) (CertificateBundle, error) {
	bundle := CertificateBundle{
		Root:                 root,
		CACertificate:        filepath.Join(root, "ca.crt"),
		ServerCertificate:    filepath.Join(root, "server.crt"),
		ServerPrivateKey:     filepath.Join(root, "server.key"),
		GatewayCertificate:   filepath.Join(root, "gateway", "client.crt"),
		GatewayPrivateKey:    filepath.Join(root, "gateway", "client.key"),
		BackendCertificate:   filepath.Join(root, "backend", "client.crt"),
		BackendPrivateKey:    filepath.Join(root, "backend", "client.key"),
		BootstrapCertificate: filepath.Join(root, "bootstrap", "client.crt"),
		BootstrapPrivateKey:  filepath.Join(root, "bootstrap", "client.key"),
	}
	for _, path := range []string{
		bundle.CACertificate,
		bundle.ServerCertificate,
		bundle.ServerPrivateKey,
		bundle.GatewayCertificate,
		bundle.GatewayPrivateKey,
		bundle.BackendCertificate,
		bundle.BackendPrivateKey,
		bundle.BootstrapCertificate,
		bundle.BootstrapPrivateKey,
	} {
		if _, err := os.Stat(path); err == nil {
			return CertificateBundle{}, fmt.Errorf("certificate bundle file already exists: %s", path)
		} else if !os.IsNotExist(err) {
			return CertificateBundle{}, err
		}
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return CertificateBundle{}, err
	}
	now := time.Now().UTC()
	caSerial, err := randomSerial()
	if err != nil {
		return CertificateBundle{}, err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "WeKnora Engine Controller CA"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return CertificateBundle{}, err
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	type generatedLeaf struct {
		certificatePath string
		privateKeyPath  string
		certificatePEM  []byte
		privateKeyPEM   []byte
	}
	leaves := make([]generatedLeaf, 0, 4)
	serverCert, serverKey, err := createLeaf(caTemplate, caKey, leafOptions{
		commonName: "weknora-engine-host-controller",
		server:     true,
		dnsNames:   []string{"host.docker.internal", "localhost"},
		ipAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
		},
	})
	if err != nil {
		return CertificateBundle{}, err
	}
	leaves = append(leaves, generatedLeaf{bundle.ServerCertificate, bundle.ServerPrivateKey, serverCert, serverKey})
	for _, client := range []struct {
		commonName      string
		certificatePath string
		privateKeyPath  string
	}{
		{ClientCNGateway, bundle.GatewayCertificate, bundle.GatewayPrivateKey},
		{ClientCNBackend, bundle.BackendCertificate, bundle.BackendPrivateKey},
		{ClientCNBootstrap, bundle.BootstrapCertificate, bundle.BootstrapPrivateKey},
	} {
		certificate, privateKey, leafErr := createLeaf(caTemplate, caKey, leafOptions{commonName: client.commonName})
		if leafErr != nil {
			return CertificateBundle{}, leafErr
		}
		leaves = append(leaves, generatedLeaf{
			client.certificatePath,
			client.privateKeyPath,
			certificate,
			privateKey,
		})
	}

	created := make([]string, 0, 9)
	cleanup := func() {
		for index := len(created) - 1; index >= 0; index-- {
			_ = os.Remove(created[index])
		}
	}
	write := func(path string, contents []byte) error {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		created = append(created, path)
		if _, err := file.Write(contents); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
		return file.Close()
	}
	if err := write(bundle.CACertificate, caPEM); err != nil {
		cleanup()
		return CertificateBundle{}, err
	}
	for _, leaf := range leaves {
		if err := write(leaf.certificatePath, leaf.certificatePEM); err != nil {
			cleanup()
			return CertificateBundle{}, err
		}
		if err := write(leaf.privateKeyPath, leaf.privateKeyPEM); err != nil {
			cleanup()
			return CertificateBundle{}, err
		}
	}
	return bundle, nil
}

type leafOptions struct {
	commonName  string
	server      bool
	dnsNames    []string
	ipAddresses []net.IP
}

func createLeaf(
	ca *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	options leafOptions,
) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now().UTC()
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: options.commonName},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		DNSNames:     options.dnsNames,
		IPAddresses:  options.ipAddresses,
	}
	if options.server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER}), nil
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}
