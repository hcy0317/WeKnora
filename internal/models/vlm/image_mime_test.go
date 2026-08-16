package vlm

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

const onePixelPNGBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="

func validOnePixelPNG(t *testing.T) []byte {
	t.Helper()
	data, err := base64.StdEncoding.DecodeString(onePixelPNGBase64)
	require.NoError(t, err)
	return data
}

func TestDetectImageMIMERejectsEnhancedMetafile(t *testing.T) {
	// EMF records carry the ASCII signature at offset 40. The legacy DOC
	// failure that prompted this test was named .x-wmf but had this exact type.
	emf := make([]byte, 88)
	copy(emf[40:], []byte(" EMF"))

	_, err := detectImageMIME(emf)
	require.ErrorContains(t, err, "expected JPEG, PNG, GIF, or WebP")
}

func TestDetectImageMIMEAcceptsRealPNG(t *testing.T) {
	mimeType, err := detectImageMIME(validOnePixelPNG(t))
	require.NoError(t, err)
	require.Equal(t, "image/png", mimeType)
}
