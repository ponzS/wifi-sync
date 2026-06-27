package main

import (
	"testing"
	"time"
)

func TestApplyTextOperationUTF16(t *testing.T) {
	got, err := applyTextOperation("A🙂B", 1, 3, "中")
	if err != nil {
		t.Fatalf("applyTextOperation returned error: %v", err)
	}
	if got != "A中B" {
		t.Fatalf("applyTextOperation = %q, want %q", got, "A中B")
	}
}

func TestTransformOperationTieBreak(t *testing.T) {
	applied := boardOperationEvent{Start: 0, End: 0, Text: "R"}
	local := boardOperationEvent{Start: 0, End: 0, Text: "L"}

	after := transformOperation(local, applied, true)
	if after.Start != 1 || after.End != 1 {
		t.Fatalf("transformOperation after = %+v, want start/end at 1", after)
	}

	before := transformOperation(applied, local, false)
	if before.Start != 0 || before.End != 0 {
		t.Fatalf("transformOperation before = %+v, want start/end at 0", before)
	}
}

func TestBoardStoreApplyRebasesConcurrentInsert(t *testing.T) {
	store := newBoardStore(t.TempDir() + "/board.json")

	first, err := store.apply(boardOperationRequest{
		SessionID:   "session-a",
		OperationID: "op-a",
		BaseVersion: 0,
		Start:       0,
		End:         0,
		Text:        "R",
	}, time.Now())
	if err != nil {
		t.Fatalf("first apply returned error: %v", err)
	}
	if first.Text != "R" || first.Version != 1 {
		t.Fatalf("first apply = %+v, want text/version R/1", first)
	}

	second, err := store.apply(boardOperationRequest{
		SessionID:   "session-b",
		OperationID: "op-b",
		BaseVersion: 0,
		Start:       0,
		End:         0,
		Text:        "L",
	}, time.Now())
	if err != nil {
		t.Fatalf("second apply returned error: %v", err)
	}
	if second.Text != "RL" || second.Version != 2 {
		t.Fatalf("second apply = %+v, want text/version RL/2", second)
	}
}
