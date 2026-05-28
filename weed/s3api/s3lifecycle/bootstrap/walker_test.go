package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// recorder captures dispatched (action, entry) pairs for assertion.
type recorder struct {
	calls         []dispatchCall
	err           error // when set, every Delete returns this error
	annotateCalls []annotateCall
	annotateErr   error // when set, every Annotate returns this error
}

type dispatchCall struct {
	kind s3lifecycle.ActionKind
	path string
}

type annotateCall struct {
	bucket    string
	path      string
	expiresAt time.Time
	ruleID    string
}

func (r *recorder) Delete(ctx context.Context, action *engine.CompiledAction, entry *Entry) error {
	if r.err != nil {
		return r.err
	}
	r.calls = append(r.calls, dispatchCall{kind: action.Key.ActionKind, path: entry.Path})
	return nil
}

func (r *recorder) Annotate(_ context.Context, bucket string, entry *Entry, expiresAt time.Time, ruleID string) error {
	if r.annotateErr != nil {
		return r.annotateErr
	}
	r.annotateCalls = append(r.annotateCalls, annotateCall{
		bucket:    bucket,
		path:      entry.Path,
		expiresAt: expiresAt,
		ruleID:    ruleID,
	})
	return nil
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return tm
}

func compileEvDriven(t *testing.T, bucket string, rules ...*s3lifecycle.Rule) *engine.Snapshot {
	t.Helper()
	prior := map[s3lifecycle.ActionKey]engine.PriorState{}
	for _, r := range rules {
		rh := s3lifecycle.RuleHash(r)
		for _, k := range s3lifecycle.RuleActionKinds(r) {
			prior[s3lifecycle.ActionKey{Bucket: bucket, RuleHash: rh, ActionKind: k}] = engine.PriorState{BootstrapComplete: true}
		}
	}
	e := engine.New()
	return e.Compile([]engine.CompileInput{{Bucket: bucket, Rules: rules}}, engine.CompileOptions{PriorStates: prior})
}

func TestWalk_DispatchesDueActions(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
		Prefix:         "logs/",
	}
	snap := compileEvDriven(t, "bk", rule)

	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 60) // past the 30d threshold
	entries := []*Entry{
		{Path: "data/x", IsLatest: true, ModTime: mod}, // wrong prefix
		{Path: "logs/a", IsLatest: true, ModTime: mod}, // due
		{Path: "logs/b", IsLatest: true, ModTime: now}, // not yet due (mod=now)
	}

	rec := &recorder{}
	cp, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if !cp.Completed {
		t.Fatalf("walk should complete")
	}
	if cp.LastScannedPath != "logs/b" {
		t.Fatalf("checkpoint last scanned want logs/b, got %q", cp.LastScannedPath)
	}
	if len(rec.calls) != 1 || rec.calls[0].path != "logs/a" {
		t.Fatalf("dispatched calls want [logs/a], got %v", rec.calls)
	}
}

func TestWalk_MultiActionRule_AllDueDispatched(t *testing.T) {
	// One rule with three actions; all currently-due for the entry. The
	// walker must dispatch one Delete per action — this is the
	// regression that the per-action keying fixes.
	rule := &s3lifecycle.Rule{
		ID:                              "multi",
		Status:                          s3lifecycle.StatusEnabled,
		ExpirationDays:                  30,
		NoncurrentVersionExpirationDays: 7,
		AbortMPUDaysAfterInitiation:     5,
	}
	snap := compileEvDriven(t, "bk", rule)

	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 100) // past every threshold
	// Use a single entry that satisfies all three action shapes is
	// unrealistic; in practice each shape is a different entry. Cover
	// each shape independently.
	entries := []*Entry{
		// Current version under ExpirationDays.
		{Path: "obj/a", IsLatest: true, ModTime: mod},
		// Non-current version under NoncurrentDays.
		{Path: "obj/a/.versions/v1", IsLatest: false, ModTime: mod, SuccessorModTime: mod},
		// MPU init under AbortMPU. Real init is a directory; DestKey
		// carries the eventual object key for prefix matching.
		{Path: ".uploads/u1", IsDirectory: true, IsMPUInit: true, DestKey: "obj/a", ModTime: mod},
	}

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}

	// Exact-shape assertion: each entry dispatches exactly one action,
	// and ABORT_MPU only fires on the .uploads/<id> entry. A weaker
	// "kinds-as-set" check would have missed the (kind, info) gating
	// regression where an MPU init also fired NONCURRENT_DAYS.
	want := []dispatchCall{
		{kind: s3lifecycle.ActionKindAbortMPU, path: ".uploads/u1"},
		{kind: s3lifecycle.ActionKindExpirationDays, path: "obj/a"},
		{kind: s3lifecycle.ActionKindNoncurrentDays, path: "obj/a/.versions/v1"},
	}
	got := append([]dispatchCall(nil), rec.calls...)
	sort.Slice(got, func(i, j int) bool {
		if got[i].path != got[j].path {
			return got[i].path < got[j].path
		}
		return got[i].kind < got[j].kind
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatch calls mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestWalk_NotYetDueSkipped(t *testing.T) {
	// The reader (Phase 3) is responsible for not-yet-due entries; the
	// walker dispatches only currently-due ones, so the meta-log path
	// stays the steady-state route.
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.Add(s3lifecycle.DaysToDuration(10)) // before the 30d threshold

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("not-yet-due entry should not dispatch, got %v", rec.calls)
	}
}

func TestWalk_DateActionFiresAfterDate(t *testing.T) {
	// The dedicated SCAN_AT_DATE bootstrap was never wired; until it
	// lands, the regular bootstrap walker is the only path that fires
	// ExpirationDate rules. Entries past the rule's date must dispatch.
	date := mustTime(t, "2025-06-15T00:00:00Z")
	rule := &s3lifecycle.Rule{
		ID:             "d",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: date,
	}
	snap := compileEvDriven(t, "bk", rule)

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")},
	}), rec, WalkOptions{Now: date.AddDate(0, 1, 0)}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 1 || rec.calls[0].path != "x/a" {
		t.Fatalf("date kind past the rule date must dispatch, got %v", rec.calls)
	}
}

func TestWalk_DateActionSkippedBeforeDate(t *testing.T) {
	// Pre-date walks are no-ops: EvaluateAction returns ActionNone when
	// now is before rule.ExpirationDate.
	date := mustTime(t, "2025-06-15T00:00:00Z")
	rule := &s3lifecycle.Rule{
		ID:             "d",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: date,
	}
	snap := compileEvDriven(t, "bk", rule)

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")},
	}), rec, WalkOptions{Now: date.AddDate(0, -1, 0)}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("pre-date walk must not dispatch, got %v", rec.calls)
	}
}

func TestWalk_DirectoryEntriesSkipped(t *testing.T) {
	// SeaweedFS directory entries can co-exist in the listing; the walker
	// must not dispatch deletes against them even when their path matches.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x", IsDirectory: true, ModTime: mod}, // directory; must skip
		{Path: "x/file", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 1 || rec.calls[0].path != "x/file" {
		t.Fatalf("only the file should dispatch, got %v", rec.calls)
	}
}

func TestWalk_DisabledModeSkipped(t *testing.T) {
	// An operator-flipped ModeDisabled must short-circuit the walker even
	// when the XML rule status is "Enabled" and EvaluateAction would
	// otherwise fire.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	rh := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]engine.PriorState{
		{Bucket: "bk", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}: {
			BootstrapComplete: true, Mode: engine.ModeDisabled,
		},
	}
	e := engine.New()
	snap := e.Compile([]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}}, engine.CompileOptions{PriorStates: prior})
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("disabled action must not dispatch, got %v", rec.calls)
	}
}

func TestWalk_PendingBootstrapNotDispatched(t *testing.T) {
	// Without bootstrap_complete=true in PriorStates, the engine compiles
	// the action as inactive. MatchPath filters on IsActive, so the
	// walker won't dispatch.
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 1,
	}
	e := engine.New()
	snap := e.Compile([]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}}, engine.CompileOptions{})

	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("inactive action should not dispatch, got %v", rec.calls)
	}
}

func TestWalk_DispatchErrorHaltsAtCheckpoint(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 1,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	entries := []*Entry{
		{Path: "a", IsLatest: true, ModTime: mod},
		{Path: "b", IsLatest: true, ModTime: mod},
		{Path: "c", IsLatest: true, ModTime: mod},
	}

	wantErr := errors.New("dispatch boom")
	rec := &recorder{err: wantErr}
	cp, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want dispatch error, got %v", err)
	}
	if cp.Completed {
		t.Fatalf("walk should not be Completed on dispatch failure")
	}
	// Walker stops on first failure; checkpoint stays at whatever was
	// recorded BEFORE the failed entry. Path "a" is the failing entry,
	// so LastScannedPath stays at the resume point (empty here).
	if cp.LastScannedPath != "" {
		t.Fatalf("checkpoint should not advance past failing entry, got %q", cp.LastScannedPath)
	}
}

func TestWalk_ResumeFromCheckpoint(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 1,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	entries := []*Entry{
		{Path: "a", IsLatest: true, ModTime: mod},
		{Path: "b", IsLatest: true, ModTime: mod},
		{Path: "c", IsLatest: true, ModTime: mod},
	}

	rec := &recorder{}
	cp, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now, Resume: "b"})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if !cp.Completed {
		t.Fatalf("walk should complete")
	}
	// Only "c" is processed (entries with Path <= "b" are skipped).
	if len(rec.calls) != 1 || rec.calls[0].path != "c" {
		t.Fatalf("Resume should only process c, got %v", rec.calls)
	}
	if cp.LastScannedPath != "c" {
		t.Fatalf("checkpoint want c, got %q", cp.LastScannedPath)
	}
}

func TestWalk_MPUInitDirMatchesByDestKey(t *testing.T) {
	// Existing in-flight MPUs predate the meta-log subscription, so they
	// only get cleaned up via the bootstrap walk. The init record is a
	// directory whose path is .uploads/<id>; the rule's Filter.Prefix
	// applies to the destination object key, not the upload directory.
	rule := &s3lifecycle.Rule{
		ID:                          "r-mpu",
		Status:                      s3lifecycle.StatusEnabled,
		Prefix:                      "logs/",
		AbortMPUDaysAfterInitiation: 7,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 8) // past the 7d threshold

	entries := []*Entry{
		// Matches: dest key under logs/.
		{Path: ".uploads/u-match", IsDirectory: true, IsMPUInit: true, DestKey: "logs/foo.txt", ModTime: mod},
		// Filtered out: dest key under data/.
		{Path: ".uploads/u-skip", IsDirectory: true, IsMPUInit: true, DestKey: "data/foo.txt", ModTime: mod},
		// No DestKey: malformed init mid-write; skip rather than guess.
		{Path: ".uploads/u-bare", IsDirectory: true, IsMPUInit: true, ModTime: mod},
	}

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("expected 1 dispatch (u-match only), got %v", rec.calls)
	}
	if rec.calls[0].path != ".uploads/u-match" {
		t.Fatalf("dispatch path=%q, want .uploads/u-match (the rm target)", rec.calls[0].path)
	}
	if rec.calls[0].kind != s3lifecycle.ActionKindAbortMPU {
		t.Fatalf("dispatch kind=%v, want AbortMPU", rec.calls[0].kind)
	}
}

func TestWalk_NonMPUDirectorySkipped(t *testing.T) {
	// Non-MPU directories must still be skipped — the relaxed
	// IsDirectory check is gated on IsMPUInit.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 100)

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "a/", IsDirectory: true, IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("plain directory should not dispatch, got %v", rec.calls)
	}
}

func TestWalk_NotYetDueExpirationDaysAnnotates(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "exp-rule",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.Add(s3lifecycle.DaysToDuration(10)) // 10d in, not yet due at 30d

	rec := &recorder{}
	_, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "logs/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("not-yet-due entry must not dispatch delete, got %v", rec.calls)
	}
	if len(rec.annotateCalls) != 1 {
		t.Fatalf("want 1 Annotate call, got %d", len(rec.annotateCalls))
	}
	ac := rec.annotateCalls[0]
	if ac.bucket != "bk" {
		t.Fatalf("annotate bucket want bk, got %q", ac.bucket)
	}
	if ac.path != "logs/a" {
		t.Fatalf("annotate path want logs/a, got %q", ac.path)
	}
	if ac.ruleID != "exp-rule" {
		t.Fatalf("annotate ruleID want exp-rule, got %q", ac.ruleID)
	}
	wantExpiry := mod.Add(s3lifecycle.DaysToDuration(30))
	if !ac.expiresAt.Equal(wantExpiry) {
		t.Fatalf("annotate expiresAt want %v, got %v", wantExpiry, ac.expiresAt)
	}
}

func TestWalk_NotYetDueDateAnnotates(t *testing.T) {
	expDate := mustTime(t, "2099-01-01T00:00:00Z")
	rule := &s3lifecycle.Rule{
		ID:             "date-rule",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: expDate,
	}
	snap := compileEvDriven(t, "bk", rule)
	now := mustTime(t, "2024-06-01T00:00:00Z") // well before the expiration date

	rec := &recorder{}
	_, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "obj/a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")},
	}), rec, WalkOptions{Now: now})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("pre-date entry must not dispatch delete, got %v", rec.calls)
	}
	if len(rec.annotateCalls) != 1 {
		t.Fatalf("want 1 Annotate call, got %d", len(rec.annotateCalls))
	}
	if !rec.annotateCalls[0].expiresAt.Equal(expDate) {
		t.Fatalf("annotate expiresAt want %v, got %v", expDate, rec.annotateCalls[0].expiresAt)
	}
	if rec.annotateCalls[0].ruleID != "date-rule" {
		t.Fatalf("annotate ruleID want date-rule, got %q", rec.annotateCalls[0].ruleID)
	}
}

func TestWalk_DueActionDoesNotAnnotate(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "exp-rule",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.Add(s3lifecycle.DaysToDuration(60)) // well past the 30d threshold

	rec := &recorder{}
	_, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "obj/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 1 || rec.calls[0].path != "obj/a" {
		t.Fatalf("due entry must dispatch delete, got %v", rec.calls)
	}
	if len(rec.annotateCalls) != 0 {
		t.Fatalf("dispatched-for-delete entry must not be annotated, got %v", rec.annotateCalls)
	}
}

func TestWalk_EarliestExpirationAnnotated(t *testing.T) {
	// Two rules both matching the object; r2 expires sooner. Walker must
	// annotate with the earliest expiry date and its rule ID.
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	r1 := &s3lifecycle.Rule{
		ID:             "r1",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30, // expires in 30d
	}
	r2 := &s3lifecycle.Rule{
		ID:             "r2",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 10, // expires in 10d — earliest
	}
	snap := compileEvDriven(t, "bk", r1, r2)
	now := mod.Add(s3lifecycle.DaysToDuration(5)) // 5d in, neither is due

	rec := &recorder{}
	_, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "obj/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("neither rule is due, no delete expected, got %v", rec.calls)
	}
	if len(rec.annotateCalls) != 1 {
		t.Fatalf("want exactly 1 Annotate call, got %d", len(rec.annotateCalls))
	}
	wantExpiry := mod.Add(s3lifecycle.DaysToDuration(10))
	if !rec.annotateCalls[0].expiresAt.Equal(wantExpiry) {
		t.Fatalf("want earliest expiry %v, got %v", wantExpiry, rec.annotateCalls[0].expiresAt)
	}
	if rec.annotateCalls[0].ruleID != "r2" {
		t.Fatalf("want ruleID r2 (earliest), got %q", rec.annotateCalls[0].ruleID)
	}
}

func TestWalk_AnnotateErrorIsNonFatal(t *testing.T) {
	// Annotate failure must not halt the walk — a missing annotation is
	// a missing response header, not a data-loss event.
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.Add(s3lifecycle.DaysToDuration(5)) // not yet due

	rec := &recorder{annotateErr: errors.New("filer write failed")}
	cp, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "obj/a", IsLatest: true, ModTime: mod},
		{Path: "obj/b", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now})
	if err != nil {
		t.Fatalf("Walk must not fail on Annotate error, got %v", err)
	}
	if !cp.Completed {
		t.Fatalf("walk should complete despite Annotate error")
	}
	if cp.LastScannedPath != "obj/b" {
		t.Fatalf("walk should scan all entries, checkpoint want obj/b, got %q", cp.LastScannedPath)
	}
}

func TestWalk_MPUInitDoesNotFireNoncurrent(t *testing.T) {
	// Same rule covers both AbortMPU and NoncurrentVersionExpiration; the
	// MPU init record must dispatch only the AbortMPU action. Without the
	// engine guard, NONCURRENT_DAYS would fire (IsLatest=false) and the
	// server would BLOCK on empty version_id, freezing the cursor.
	rule := &s3lifecycle.Rule{
		ID:                              "r",
		Status:                          s3lifecycle.StatusEnabled,
		AbortMPUDaysAfterInitiation:     7,
		NoncurrentVersionExpirationDays: 7,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 30)

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: ".uploads/u1", IsDirectory: true, IsMPUInit: true, DestKey: "obj/a", ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("expected 1 dispatch (AbortMPU only), got %v", rec.calls)
	}
	if rec.calls[0].kind != s3lifecycle.ActionKindAbortMPU {
		t.Fatalf("kind=%v, want AbortMPU", rec.calls[0].kind)
	}
}
