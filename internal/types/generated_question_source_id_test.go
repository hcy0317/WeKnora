package types

import (
	"strings"
	"testing"
)

func TestGeneratedQuestionSourceID(t *testing.T) {
	chunkID := "135bf11d-c20e-419a-9f3c-5288d6a0516b"
	shortQuestionID := "q1722147984000000000"
	if got, want := GeneratedQuestionSourceID(chunkID, shortQuestionID), chunkID+"-"+shortQuestionID; got != want {
		t.Fatalf("short source ID changed: got %q, want %q", got, want)
	}

	longQuestionID := "a4fa83a2-0fde-4b85-99bc-4572fa92d51c"
	got := GeneratedQuestionSourceID(chunkID, longQuestionID)
	if len(got) > maxGeneratedQuestionSourceIDLength {
		t.Fatalf("source ID length = %d, want <= %d: %q", len(got), maxGeneratedQuestionSourceIDLength, got)
	}
	if !strings.HasPrefix(got, chunkID+"-q") {
		t.Fatalf("hashed source ID has unexpected format: %q", got)
	}
	if got != GeneratedQuestionSourceID(chunkID, longQuestionID) {
		t.Fatal("source ID hashing is not deterministic")
	}
}

func TestStableGeneratedQuestionIDUsesWholeLogicalIdentity(t *testing.T) {
	base := StableGeneratedQuestionID("knowledge", "chunk", 4, 3, 2)
	if base == "" {
		t.Fatal("stable question ID must not be empty")
	}
	if got := StableGeneratedQuestionID("knowledge", "chunk", 4, 3, 2); got != base {
		t.Fatalf("same logical identity changed: got %q want %q", got, base)
	}
	cases := []string{
		StableGeneratedQuestionID("knowledge-2", "chunk", 4, 3, 2),
		StableGeneratedQuestionID("knowledge", "chunk-2", 4, 3, 2),
		StableGeneratedQuestionID("knowledge", "chunk", 5, 3, 2),
		StableGeneratedQuestionID("knowledge", "chunk", 4, 4, 2),
		StableGeneratedQuestionID("knowledge", "chunk", 4, 3, 3),
	}
	for _, candidate := range cases {
		if candidate == base {
			t.Fatalf("distinct logical identity collided with %q", base)
		}
	}
	chunkID := "135bf11d-c20e-419a-9f3c-5288d6a0516b"
	if got := GeneratedQuestionSourceID(chunkID, base); len(got) > maxGeneratedQuestionSourceIDLength {
		t.Fatalf("source ID is too long: %d %q", len(got), got)
	}
}
