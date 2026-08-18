package hostcontroller

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapCertificateBundleCreatesFixedCapabilitiesWithoutOverwrite(t *testing.T) {
	t.Parallel()

	bundle, err := BootstrapCertificateBundle(t.TempDir())
	require.NoError(t, err)

	server := readCertificate(t, bundle.ServerCertificate)
	require.Contains(t, server.DNSNames, "host.docker.internal")
	require.Contains(t, server.DNSNames, "localhost")
	require.Equal(t, "weknora-engine-host-controller", server.Subject.CommonName)

	require.Equal(t, ClientCNGateway, readCertificate(t, bundle.GatewayCertificate).Subject.CommonName)
	require.Equal(t, ClientCNBackend, readCertificate(t, bundle.BackendCertificate).Subject.CommonName)
	require.Equal(t, ClientCNBootstrap, readCertificate(t, bundle.BootstrapCertificate).Subject.CommonName)

	_, err = BootstrapCertificateBundle(bundle.Root)
	require.ErrorContains(t, err, "already exists")
}

func readCertificate(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	block, _ := pem.Decode(contents)
	require.NotNil(t, block)
	certificate, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return certificate
}
