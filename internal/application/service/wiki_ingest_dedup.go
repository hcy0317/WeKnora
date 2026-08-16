package service

import (
	"sort"
	"strings"
	"unicode"

	"github.com/Tencent/WeKnora/internal/searchutil"
	"github.com/Tencent/WeKnora/internal/types"
)

// Pre-filtering candidate existing pages before the dedup LLM call.
//
// Without pre-filtering, the dedup prompt packs the entire entity+concept
// page corpus into <existing_pages>. On knowledge bases with 100+ pages
// this inflates input tokens and — more importantly — gives weaker LLMs
// enough rope to hallucinate merges between totally unrelated slugs just
// because the output looked plausible. Observed cases include
// "城镇登记失业人员" → "中华优秀传统文化" (zero shared characters).
//
// The filter below keeps only pages that share at least some cheap
// surface-level signal with one of the new items. Fast to compute, no
// external calls, and it only ever *removes* candidates from the prompt —
// the downstream validMerge check still guards the final write.
const (
	// dedupCandidateTopK bounds how many trigram-similar existing pages
	// each new item's similarity probe returns (the LIMIT passed to
	// FindSimilarPages, applied per query term = name + each alias). Kept
	// deliberately small: the DB already orders by similarity desc behind a
	// pg_trgm threshold, so a genuine same-entity target is essentially
	// always the top hit. A tight K keeps each item's <candidates> list in
	// the dedup prompt short, which improves the model's precision and cuts
	// tokens, at negligible recall cost.
	dedupCandidateTopK = 5

	// dedupCandidateScoreFloor is the Jaccard floor. Pairs at or above
	// this similarity are always included regardless of the top-K cap.
	// Tuned so that "城镇登记失业人员" vs "中华优秀传统文化" (Jaccard 0)
	// is excluded while "Acme Corp" vs "Acme Corporation" (Jaccard ≈ 0.5)
	// clearly passes.
	dedupCandidateScoreFloor = 0.08

	// dedupSmallCorpusBypass skips pre-filtering entirely when the
	// existing-page corpus is already small enough to fit in the prompt
	// without degrading the LLM. The filter only earns its keep on large
	// KBs; on small ones it risks cutting legitimate matches with no
	// real token savings.
	dedupSmallCorpusBypass = 25
)

// dedupSurface is the pre-computed similarity feature set for one side of
// a (new item, existing page) comparison.
type dedupSurface struct {
	// slugTokens are the kebab-case tokens from the slug base (after "/").
	// Slugs are an orthogonal signal to the surface names — Chinese pages
	// carry their pinyin here, which keeps the filter useful on purely
	// Latin-script new items too.
	slugTokens map[string]struct{}

	// nameGramSets holds one char-bigram set per surface form (name and
	// each alias). We keep them separate so the pair score is max-over-
	// surfaces — a rare alias match shouldn't be diluted by the primary
	// name disagreeing.
	nameGramSets []map[string]struct{}
}

// canonicalItemRecord is the small, persistence-independent shape used by
// deterministic identity compaction. preferred marks a slug that already
// exists in the wiki; an existing slug must win over a freshly generated
// spelling variant so repeated ingests cannot make the canonical slug bounce.
type canonicalItemRecord struct {
	item      extractedItem
	kind      string
	preferred bool
}

// normalizedIdentitySurface normalizes only presentation differences. It is
// deliberately stricter than the fuzzy dedup score: related subjects such as
// "玉米纹枯病" and "纹枯病" remain distinct, while "同玉609" and
// "同玉 609" compare equal.
func normalizedIdentitySurface(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func extractedItemIdentitySurfaces(item extractedItem) map[string]struct{} {
	out := make(map[string]struct{}, 1+len(item.Aliases))
	for _, value := range append([]string{item.Name}, item.Aliases...) {
		if normalized := normalizedIdentitySurface(value); normalized != "" {
			out[normalized] = struct{}{}
		}
	}
	return out
}

func wikiPageIdentitySurfaces(page *types.WikiPageLite) map[string]struct{} {
	if page == nil {
		return nil
	}
	out := make(map[string]struct{}, 1+len(page.Aliases))
	for _, value := range append([]string{page.Title}, []string(page.Aliases)...) {
		if normalized := normalizedIdentitySurface(value); normalized != "" {
			out[normalized] = struct{}{}
		}
	}
	return out
}

func identitySurfacesIntersect(left, right map[string]struct{}) bool {
	if len(left) > len(right) {
		left, right = right, left
	}
	for surface := range left {
		if _, ok := right[surface]; ok {
			return true
		}
	}
	return false
}

func slugKind(slug string) string {
	if slash := strings.Index(slug, "/"); slash > 0 {
		return strings.ToLower(slug[:slash])
	}
	return ""
}

// compactSlugIdentity removes only separators from the slug body. This catches
// pinyin segmentation drift such as tongyu-609 vs tong-yu-609 without using a
// broad fuzzy match that could collapse merely related subjects.
func compactSlugIdentity(slug string) string {
	if slash := strings.Index(slug, "/"); slash >= 0 {
		slug = slug[slash+1:]
	}
	return normalizedIdentitySurface(slug)
}

func canonicalRecordsShareIdentity(left, right canonicalItemRecord) bool {
	if left.item.Slug != "" && left.item.Slug == right.item.Slug {
		return true
	}
	if identitySurfacesIntersect(extractedItemIdentitySurfaces(left.item), extractedItemIdentitySurfaces(right.item)) {
		return true
	}
	return left.kind == right.kind && left.kind != "" &&
		compactSlugIdentity(left.item.Slug) != "" &&
		compactSlugIdentity(left.item.Slug) == compactSlugIdentity(right.item.Slug)
}

func mergeExtractedItem(dst *extractedItem, src extractedItem) {
	if dst.Name == "" || len([]rune(src.Name)) > len([]rune(dst.Name)) {
		dst.Name = src.Name
	}
	dst.Description = mergeIndependentFactText(dst.Description, src.Description)
	dst.Details = mergeIndependentFactText(dst.Details, src.Details)
	dst.Aliases = uniqueNonEmptyStrings(append(dst.Aliases, src.Aliases...))
	dst.SourceChunks = uniqueNonEmptyStrings(append(dst.SourceChunks, src.SourceChunks...))
}

// canonicalizeItemRecords groups only deterministic identities, merges their
// facts, and returns old-slug -> canonical-slug aliases. Existing slugs win;
// otherwise entities win an exact cross-type collision, then lexical slug
// order makes the result stable across goroutine/map iteration order.
func canonicalizeItemRecords(records []canonicalItemRecord) ([]canonicalItemRecord, map[string]string) {
	if len(records) == 0 {
		return nil, nil
	}

	parent := make([]int, len(records))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(index int) int {
		if parent[index] != index {
			parent[index] = find(parent[index])
		}
		return parent[index]
	}
	union := func(left, right int) {
		leftRoot, rightRoot := find(left), find(right)
		if leftRoot != rightRoot {
			parent[rightRoot] = leftRoot
		}
	}
	for i := range records {
		for j := i + 1; j < len(records); j++ {
			if canonicalRecordsShareIdentity(records[i], records[j]) {
				union(i, j)
			}
		}
	}

	components := make(map[int][]int)
	var componentOrder []int
	for index := range records {
		root := find(index)
		if _, ok := components[root]; !ok {
			componentOrder = append(componentOrder, root)
		}
		components[root] = append(components[root], index)
	}

	aliases := make(map[string]string)
	out := make([]canonicalItemRecord, 0, len(components))
	for _, root := range componentOrder {
		indexes := components[root]
		winner := indexes[0]
		better := func(candidate, current canonicalItemRecord) bool {
			if candidate.preferred != current.preferred {
				return candidate.preferred
			}
			if !candidate.preferred && candidate.kind != current.kind {
				if candidate.kind == types.WikiPageTypeEntity {
					return true
				}
				if current.kind == types.WikiPageTypeEntity {
					return false
				}
			}
			return candidate.item.Slug < current.item.Slug
		}
		for _, index := range indexes[1:] {
			if better(records[index], records[winner]) {
				winner = index
			}
		}

		canonical := records[winner]
		canonical.item = extractedItem{Slug: records[winner].item.Slug}
		for _, index := range indexes {
			mergeExtractedItem(&canonical.item, records[index].item)
			oldSlug := records[index].item.Slug
			if oldSlug != "" && oldSlug != canonical.item.Slug {
				aliases[oldSlug] = canonical.item.Slug
			}
		}
		out = append(out, canonical)
	}
	if len(aliases) == 0 {
		aliases = nil
	}
	return out, aliases
}

// compactExtractedItems removes deterministic duplicates produced inside one
// extraction run (including across 6000-rune shards and citation new_slugs).
func compactExtractedItems(entities, concepts []extractedItem) ([]extractedItem, []extractedItem) {
	records := make([]canonicalItemRecord, 0, len(entities)+len(concepts))
	for _, item := range entities {
		records = append(records, canonicalItemRecord{item: item, kind: types.WikiPageTypeEntity})
	}
	for _, item := range concepts {
		records = append(records, canonicalItemRecord{item: item, kind: types.WikiPageTypeConcept})
	}
	canonical, _ := canonicalizeItemRecords(records)

	entities = make([]extractedItem, 0, len(canonical))
	concepts = make([]extractedItem, 0, len(canonical))
	for _, record := range canonical {
		kind := slugKind(record.item.Slug)
		if kind == "" {
			kind = record.kind
		}
		if kind == types.WikiPageTypeConcept {
			concepts = append(concepts, record.item)
		} else {
			entities = append(entities, record.item)
		}
	}
	return entities, concepts
}

// deterministicExistingMergeTarget resolves exact identities before asking
// the LLM about genuinely fuzzy near-matches. Same-type targets are preferred;
// an exact title/alias match may cross the entity/concept boundary so one real
// subject cannot persist twice solely because the model changed its type tag.
func deterministicExistingMergeTarget(
	item extractedItem,
	itemType string,
	candidates map[string]bool,
	pages map[string]*types.WikiPageLite,
) string {
	itemSurfaces := extractedItemIdentitySurfaces(item)
	var sameType, crossType []string
	for slug := range candidates {
		page := pages[slug]
		if page == nil || page.Slug == "" {
			continue
		}
		pageType := page.PageType
		if pageType == "" {
			pageType = slugKind(page.Slug)
		}
		exactSurface := identitySurfacesIntersect(itemSurfaces, wikiPageIdentitySurfaces(page))
		same := pageType == itemType
		sameCompactSlug := same && compactSlugIdentity(item.Slug) != "" &&
			compactSlugIdentity(item.Slug) == compactSlugIdentity(page.Slug)
		if !exactSurface && !sameCompactSlug {
			continue
		}
		if same {
			sameType = append(sameType, page.Slug)
		} else if exactSurface {
			crossType = append(crossType, page.Slug)
		}
	}
	if len(sameType) > 0 {
		sort.Strings(sameType)
		return sameType[0]
	}
	if len(crossType) > 0 {
		sort.Strings(crossType)
		return crossType[0]
	}
	return ""
}

// countEntityConceptPages returns how many of the given pages are
// entity- or concept-typed. Used only for logging the prefilter's
// reduction ratio.
func countEntityConceptPages(pages []*types.WikiPage) int {
	n := 0
	for _, p := range pages {
		if p == nil {
			continue
		}
		if p.PageType == types.WikiPageTypeEntity || p.PageType == types.WikiPageTypeConcept {
			n++
		}
	}
	return n
}

// selectDedupCandidatePages returns the subset of allPages plausibly
// related to at least one of newItems. Non-entity/concept pages are
// dropped unconditionally. The returned slice preserves the input order
// so the downstream prompt stays stable across runs.
//
// On small corpora (<= dedupSmallCorpusBypass entries) this is a no-op
// aside from the page-type filter.
func selectDedupCandidatePages(
	newItems []extractedItem,
	allPages []*types.WikiPage,
) []*types.WikiPage {
	pages := make([]*types.WikiPage, 0, len(allPages))
	for _, p := range allPages {
		if p == nil {
			continue
		}
		if p.PageType != types.WikiPageTypeEntity && p.PageType != types.WikiPageTypeConcept {
			continue
		}
		pages = append(pages, p)
	}
	if len(pages) == 0 {
		return pages
	}
	if len(newItems) == 0 || len(pages) <= dedupSmallCorpusBypass {
		return pages
	}

	pageFeats := make([]dedupSurface, len(pages))
	for i, p := range pages {
		surfaces := make([]string, 0, 1+len(p.Aliases))
		surfaces = append(surfaces, p.Title)
		surfaces = append(surfaces, []string(p.Aliases)...)
		pageFeats[i] = dedupSurface{
			slugTokens:   slugBaseTokens(p.Slug),
			nameGramSets: gramsPerSurface(surfaces),
		}
	}

	selected := make(map[int]bool, len(pages))
	for _, it := range newItems {
		surfaces := make([]string, 0, 1+len(it.Aliases))
		surfaces = append(surfaces, it.Name)
		surfaces = append(surfaces, it.Aliases...)
		itemFeat := dedupSurface{
			slugTokens:   slugBaseTokens(it.Slug),
			nameGramSets: gramsPerSurface(surfaces),
		}
		if len(itemFeat.slugTokens) == 0 && len(itemFeat.nameGramSets) == 0 {
			continue
		}

		scores := make([]struct {
			idx   int
			score float64
		}, len(pageFeats))
		for i := range pageFeats {
			scores[i].idx = i
			scores[i].score = dedupPairScore(itemFeat, pageFeats[i])
		}
		// Stable sort so ties break deterministically by original index.
		sort.SliceStable(scores, func(i, j int) bool {
			return scores[i].score > scores[j].score
		})

		topKRemaining := dedupCandidateTopK
		for _, s := range scores {
			if s.score >= dedupCandidateScoreFloor {
				selected[s.idx] = true
				continue
			}
			// Below the floor but we still owe the LLM some candidates so
			// it can decline cleanly — fill the top-K budget with the
			// highest-scoring remaining pages, as long as the score isn't
			// flatly zero (a zero score means we have nothing in common
			// with this page and including it just invites hallucination).
			if topKRemaining > 0 && s.score > 0 {
				selected[s.idx] = true
				topKRemaining--
				continue
			}
			break
		}
	}

	out := make([]*types.WikiPage, 0, len(selected))
	for i, p := range pages {
		if selected[i] {
			out = append(out, p)
		}
	}
	return out
}

// dedupMergeRejectReason validates a single LLM-proposed merge (srcSlug →
// dstSlug) against deterministic, model-independent rules. It returns an
// empty string when the merge is allowed, or a short human-readable reason
// when it must be rejected. srcCandidates is the set of existing-page slugs
// that surfaced for srcSlug's OWN similarity probe (see itemCandidates in
// deduplicateExtractedBatch).
//
// The per-item scoping check is the key guard: the dedup prompt shows the
// model a flattened union of candidates across every new item, so nothing
// stops a weak model from pairing an item with a page that was only similar
// to a *different* item (observed: entity/tencent-open → entity/hiring-agent,
// which share no trigram signal). Requiring dstSlug to be one of srcSlug's
// own candidates rejects that entire class without depending on the model
// getting the semantic judgment right.
func dedupMergeRejectReason(srcSlug, dstSlug string, srcCandidates map[string]bool) string {
	if !srcCandidates[dstSlug] {
		// Covers both an outright hallucinated target (in no candidate
		// set at all) and a real page that was only similar to another
		// item. Either way the pair lacks a similarity signal for THIS
		// item, so it is not a safe merge.
		return "target is not a similarity candidate for this item"
	}
	srcSlash := strings.Index(srcSlug, "/")
	dstSlash := strings.Index(dstSlug, "/")
	if srcSlash <= 0 || dstSlash <= 0 {
		// A type-prefixed slug must look like "entity/foo" or
		// "concept/bar". An LLM that emits an un-prefixed slug here is
		// hallucinating; reject rather than fall through the prefix-
		// equality check (which would treat both empty prefixes as a
		// match).
		return "missing type prefix"
	}
	if srcSlug[:srcSlash+1] != dstSlug[:dstSlash+1] {
		return "type mismatch: " + srcSlug[:srcSlash+1] + " vs " + dstSlug[:dstSlash+1]
	}
	return ""
}

// dedupPairScore is the max similarity between any surface form of a and
// b (plus the slug-token similarity). Slug and name signals live in
// different symbol spaces (ASCII pinyin vs raw surface form) so we take
// the max rather than e.g. their average.
func dedupPairScore(a, b dedupSurface) float64 {
	best := searchutil.Jaccard(a.slugTokens, b.slugTokens)
	for _, ag := range a.nameGramSets {
		for _, bg := range b.nameGramSets {
			if v := searchutil.Jaccard(ag, bg); v > best {
				best = v
			}
		}
	}
	return best
}

// slugBaseTokens returns the kebab-case tokens of a slug's base component.
// "entity/beijing-nongshang-yinxing" → {"beijing", "nongshang", "yinxing"}.
func slugBaseTokens(slug string) map[string]struct{} {
	if slug == "" {
		return nil
	}
	base := slug
	if i := strings.Index(slug, "/"); i >= 0 {
		base = slug[i+1:]
	}
	base = strings.ToLower(base)
	fields := strings.FieldsFunc(base, func(r rune) bool {
		return r == '-' || r == '_' || r == '.' || unicode.IsSpace(r)
	})
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(fields))
	for _, tok := range fields {
		if tok == "" {
			continue
		}
		out[tok] = struct{}{}
	}
	return out
}

// gramsPerSurface computes a gram set per non-empty surface form.
func gramsPerSurface(surfaces []string) []map[string]struct{} {
	out := make([]map[string]struct{}, 0, len(surfaces))
	for _, s := range surfaces {
		g := surfaceGrams(s)
		if len(g) > 0 {
			out = append(out, g)
		}
	}
	return out
}

// surfaceGrams returns a character-bigram set for a surface form after
// lowercasing and stripping non-letter/digit runes. Bigrams work well
// across both CJK (where each bigram approximates a word) and Latin
// (where they catch stem overlap like "corporation" ↔ "corp"). Single-
// rune strings fall back to a 1-gram so they still contribute a signal.
func surfaceGrams(s string) map[string]struct{} {
	if s == "" {
		return nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	runes := []rune(b.String())
	if len(runes) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(runes))
	if len(runes) == 1 {
		out[string(runes)] = struct{}{}
		return out
	}
	for i := 0; i < len(runes)-1; i++ {
		out[string(runes[i:i+2])] = struct{}{}
	}
	return out
}
