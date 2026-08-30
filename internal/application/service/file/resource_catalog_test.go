package file

import (
	"context"
	"errors"
	"io"
	"mime/multipart"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type catalogStub struct {
	resource *types.StoredResource
	ref      string
	markErr  error
	restores int
	claim    time.Time
}

func (c *catalogStub) Register(
	_ context.Context,
	tenantID uint64,
	physicalPath string,
	meta interfaces.ResourceRegistration,
) (string, error) {
	c.resource = &types.StoredResource{
		ID:           "resource-1",
		Handle:       "AbCdEfGhIjKlMnOpQrStUv",
		TenantID:     tenantID,
		PhysicalPath: physicalPath,
		OriginalName: meta.OriginalName,
	}
	c.ref = types.BuildResourcePath(c.resource.Handle)
	return c.ref, nil
}

func (c *catalogStub) Resolve(_ context.Context, _ string) (*types.StoredResource, error) {
	return c.resource, nil
}

func (c *catalogStub) ResolvePath(_ context.Context, value string) (string, *types.StoredResource, error) {
	if value == c.ref {
		return c.resource.PhysicalPath, c.resource, nil
	}
	return value, nil, nil
}

func (c *catalogStub) ResolvePathForDeletion(ctx context.Context, value string) (string, *types.StoredResource, error) {
	return c.ResolvePath(ctx, value)
}

func (c *catalogStub) Bind(context.Context, string, string, string, string) error { return nil }

func (c *catalogStub) MarkDeleted(context.Context, string) (time.Time, error) {
	if c.markErr != nil {
		return time.Time{}, c.markErr
	}
	c.claim = time.Now().Add(time.Duration(c.restores+1) * time.Second)
	return c.claim, nil
}

func (c *catalogStub) ValidateDeletionClaim(_ context.Context, _ string, claim time.Time) error {
	if claim != c.claim {
		return errors.New("stale deletion claim")
	}
	return nil
}

func (c *catalogStub) RestoreActive(_ context.Context, _ string, claim time.Time) error {
	if claim != c.claim {
		return errors.New("stale deletion claim")
	}
	c.restores++
	c.claim = time.Time{}
	return nil
}

func (c *catalogStub) Release(context.Context, string, string, string) (int64, time.Time, error) {
	return 0, c.claim, nil
}
func (c *catalogStub) CreateAccessGrant(context.Context, string, time.Duration) (string, error) {
	return "GrantTokenAbCdEfGhIjKl", nil
}

func (c *catalogStub) ResolveAccessGrant(context.Context, string) (*types.StoredResource, error) {
	return c.resource, nil
}

type physicalFileStub struct {
	savedPath string
	readPath  string
	deleted   string
	deleteErr error
}

func (s *physicalFileStub) CheckConnectivity(context.Context) error { return nil }

func (s *physicalFileStub) SaveFile(context.Context, *multipart.FileHeader, uint64, string) (string, error) {
	return "", nil
}

func (s *physicalFileStub) SaveBytes(context.Context, []byte, uint64, string, bool) (string, error) {
	return s.savedPath, nil
}

func (s *physicalFileStub) GetFile(_ context.Context, path string) (io.ReadCloser, error) {
	s.readPath = path
	return io.NopCloser(strings.NewReader("body")), nil
}

func (s *physicalFileStub) GetFileURL(context.Context, string) (string, error) { return "", nil }

func (s *physicalFileStub) DeleteFile(_ context.Context, path string) error {
	s.deleted = path
	return s.deleteErr
}

func (s *physicalFileStub) CopyFile(context.Context, string, uint64, string) (string, error) {
	return "", nil
}

func TestResourceCatalogFileServiceReturnsReferenceAndResolvesReads(t *testing.T) {
	inner := &physicalFileStub{savedPath: "local://7/exports/a.png"}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog)

	ref, err := svc.SaveBytes(context.Background(), []byte("image"), 7, "a.png", false)
	require.NoError(t, err)
	require.Equal(t, "resource://AbCdEfGhIjKlMnOpQrStUv", ref)
	reader, err := svc.GetFile(context.Background(), ref)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, inner.savedPath, inner.readPath)
}

func TestResourceCatalogFileServiceReturnsShortExternalGrantURL(t *testing.T) {
	t.Setenv("APP_EXTERNAL_URL", "https://weknora.example.com/")
	inner := &physicalFileStub{savedPath: "local://7/exports/a.png"}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog)

	ref, err := svc.SaveBytes(context.Background(), []byte("image"), 7, "a.png", false)
	require.NoError(t, err)
	externalURL, err := svc.GetFileURL(context.Background(), ref)
	require.NoError(t, err)
	require.Equal(t, "https://weknora.example.com/r/GrantTokenAbCdEfGhIjKl", externalURL)
}

func TestResourceCatalogFileServiceClaimsDeletionBeforePhysicalDelete(t *testing.T) {
	inner := &physicalFileStub{savedPath: "local://7/exports/a.png"}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog)
	ref, err := svc.SaveBytes(context.Background(), []byte("image"), 7, "a.png", false)
	require.NoError(t, err)

	catalog.markErr = errors.New("resource is still bound")
	require.ErrorIs(t, svc.DeleteFile(context.Background(), ref), catalog.markErr)
	require.Empty(t, inner.deleted, "provider bytes must survive a losing deletion claim")

	catalog.markErr = nil
	require.NoError(t, svc.DeleteFile(context.Background(), ref))
	require.Equal(t, inner.savedPath, inner.deleted)
}

func TestResourceCatalogFileServiceRestoresTombstoneAfterProviderFailure(t *testing.T) {
	inner := &physicalFileStub{savedPath: "local://7/exports/retry.png", deleteErr: errors.New("provider unavailable")}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog)
	ref, err := svc.SaveBytes(context.Background(), []byte("image"), 7, "retry.png", false)
	require.NoError(t, err)

	require.ErrorIs(t, svc.DeleteFile(context.Background(), ref), inner.deleteErr)
	require.Equal(t, 1, catalog.restores, "failed physical deletion must make the resource retryable")

	inner.deleteErr = nil
	require.NoError(t, svc.DeleteFile(context.Background(), ref))
}

func TestResourceCatalogFileServiceRejectsStaleDeletionClaim(t *testing.T) {
	inner := &physicalFileStub{savedPath: "local://7/exports/stale.png"}
	catalog := &catalogStub{}
	svc := NewResourceCatalogFileService(inner, catalog)
	ref, err := svc.SaveBytes(context.Background(), []byte("image"), 7, "stale.png", false)
	require.NoError(t, err)
	catalog.claim = time.Now()

	staleCtx := interfaces.WithResourceDeletionClaim(context.Background(), ref, catalog.claim.Add(time.Second))
	require.ErrorContains(t, svc.DeleteFile(staleCtx, ref), "stale deletion claim")
	require.Empty(t, inner.deleted)

	winnerCtx := interfaces.WithResourceDeletionClaim(context.Background(), ref, catalog.claim)
	require.NoError(t, svc.DeleteFile(winnerCtx, ref))
	require.Equal(t, inner.savedPath, inner.deleted)
}
