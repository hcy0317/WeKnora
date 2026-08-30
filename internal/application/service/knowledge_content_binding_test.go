package service

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
)

// resolvingCatalog answers Resolve from a fixed handle → tenant map and records
// every Bind, which is all bindContentResources exercises.
type resolvingCatalog struct {
	tenantByRef map[string]uint64
	binds       []bindCall
	releases    []string
	remaining   map[string]int64
}

func (c *resolvingCatalog) Resolve(_ context.Context, ref string) (*types.StoredResource, error) {
	tenantID, ok := c.tenantByRef[ref]
	if !ok {
		return nil, errors.New("resource not found")
	}
	handle, _ := types.ParseResourcePath(ref)
	return &types.StoredResource{ID: handle, Handle: handle, TenantID: tenantID}, nil
}

func (c *resolvingCatalog) Bind(_ context.Context, ref, ownerType, ownerID, relation string) error {
	c.binds = append(c.binds, bindCall{ref, ownerType, ownerID, relation})
	return nil
}

func (c *resolvingCatalog) Register(
	context.Context, uint64, string, interfaces.ResourceRegistration,
) (string, error) {
	return "", errors.New("unused")
}

func (c *resolvingCatalog) ResolvePath(
	_ context.Context, value string,
) (string, *types.StoredResource, error) {
	return value, nil, nil
}

func (c *resolvingCatalog) ResolvePathForDeletion(
	ctx context.Context, value string,
) (string, *types.StoredResource, error) {
	return c.ResolvePath(ctx, value)
}

func (c *resolvingCatalog) Release(_ context.Context, ref, _, _ string) (int64, time.Time, error) {
	c.releases = append(c.releases, ref)
	return c.remaining[ref], time.Now(), nil
}

func (c *resolvingCatalog) MarkDeleted(context.Context, string) (time.Time, error) {
	return time.Now(), nil
}

func (c *resolvingCatalog) ValidateDeletionClaim(context.Context, string, time.Time) error {
	return nil
}

func (c *resolvingCatalog) RestoreActive(context.Context, string, time.Time) error { return nil }

func (c *resolvingCatalog) CreateAccessGrant(
	context.Context, string, time.Duration,
) (string, error) {
	return "", errors.New("unused")
}

func (c *resolvingCatalog) ResolveAccessGrant(
	context.Context, string,
) (*types.StoredResource, error) {
	return nil, errors.New("unused")
}

func contentRef(char string) string {
	return types.BuildResourcePath(strings.Repeat(char, types.ResourceHandleLength))
}

// Saving a chat answer into the knowledge base must claim the files it shows
// without copying them: the assistant message keeps its own claim, so either
// side can be deleted without breaking the other.
func TestBindContentResourcesClaimsEveryReferencedFile(t *testing.T) {
	chart := contentRef("a")
	table := contentRef("b")
	catalog := &resolvingCatalog{tenantByRef: map[string]uint64{chart: 7, table: 7}}
	svc := &knowledgeService{resourceCatalog: catalog}

	content := "## 结论\n\n![评分](" + chart + ")\n\n数据见 [表格](" + table + ")"
	svc.bindContentResources(context.Background(), 7, "kn-1", content)

	if len(catalog.binds) != 2 {
		t.Fatalf("binds = %v, want one per reference", catalog.binds)
	}
	for i, want := range []string{chart, table} {
		got := catalog.binds[i]
		if got.ref != want {
			t.Fatalf("binds[%d].ref = %q, want %q", i, got.ref, want)
		}
		if got.ownerType != types.ResourceOwnerKnowledge || got.ownerID != "kn-1" {
			t.Fatalf("binds[%d] owner = (%q, %q), want (%q, kn-1)",
				i, got.ownerType, got.ownerID, types.ResourceOwnerKnowledge)
		}
		if got.relation != types.ResourceRelationAttachment {
			t.Fatalf("binds[%d].relation = %q, want %q",
				i, got.relation, types.ResourceRelationAttachment)
		}
	}
}

// A handle pasted from another workspace must not be claimed: binding it would
// hand the caller a file it may not read.
func TestBindContentResourcesSkipsForeignAndUnknownHandles(t *testing.T) {
	mine := contentRef("c")
	theirs := contentRef("d")
	unknown := contentRef("e")
	catalog := &resolvingCatalog{tenantByRef: map[string]uint64{mine: 7, theirs: 9}}
	svc := &knowledgeService{resourceCatalog: catalog}

	content := "![a](" + mine + ") ![b](" + theirs + ") ![c](" + unknown + ")"
	svc.bindContentResources(context.Background(), 7, "kn-1", content)

	if len(catalog.binds) != 1 || catalog.binds[0].ref != mine {
		t.Fatalf("binds = %v, want only %q", catalog.binds, mine)
	}
}

func TestBindContentResourcesIsInertWithoutCatalog(t *testing.T) {
	svc := &knowledgeService{}
	// Must not panic; a deployment without a resource registry has nothing to
	// claim and keeps the pre-binding behaviour.
	svc.bindContentResources(context.Background(), 7, "kn-1", "![a]("+contentRef("f")+")")
}

type contentDeleteFileService struct{ deleted []string }

func (*contentDeleteFileService) CheckConnectivity(context.Context) error { return nil }

func (*contentDeleteFileService) SaveFile(context.Context, *multipart.FileHeader, uint64, string) (string, error) {
	return "", errors.New("unused")
}

func (*contentDeleteFileService) SaveBytes(context.Context, []byte, uint64, string, bool) (string, error) {
	return "", errors.New("unused")
}

func (*contentDeleteFileService) GetFile(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("unused")
}

func (*contentDeleteFileService) GetFileURL(context.Context, string) (string, error) {
	return "", errors.New("unused")
}

func (f *contentDeleteFileService) DeleteFile(_ context.Context, ref string) error {
	f.deleted = append(f.deleted, ref)
	return nil
}

func (*contentDeleteFileService) CopyFile(context.Context, string, uint64, string) (string, error) {
	return "", errors.New("unused")
}

func TestSyncContentResourcesReleasesOnlyRemovedHandles(t *testing.T) {
	removed := contentRef("g")
	kept := contentRef("h")
	added := contentRef("i")
	catalog := &resolvingCatalog{
		tenantByRef: map[string]uint64{removed: 7, kept: 7, added: 7},
		remaining:   map[string]int64{removed: 0},
	}
	files := &contentDeleteFileService{}
	svc := &knowledgeService{resourceCatalog: catalog}

	svc.syncContentResources(
		context.Background(), 7, "kn-1",
		"![removed]("+removed+") ![kept]("+kept+")",
		"![kept]("+kept+") ![added]("+added+")",
		files,
	)

	if len(catalog.releases) != 1 || catalog.releases[0] != removed {
		t.Fatalf("releases = %v, want only %q", catalog.releases, removed)
	}
	if len(files.deleted) != 1 || files.deleted[0] != removed {
		t.Fatalf("deleted = %v, want only %q", files.deleted, removed)
	}
	if len(catalog.binds) != 2 {
		t.Fatalf("binds = %v, want kept and added handles", catalog.binds)
	}
}
