// Package bootstrap is the bucket-level lifecycle walker. The walker
// iterates every entry under a bucket, evaluates every active ActionKey
// against it, and dispatches inline-delete RPCs for currently-due actions;
// not-yet-due entries are left for the meta-log reader to pick up later.
//
// Callback-driven so the listing source and the LifecycleDelete dispatcher
// can be supplied separately (real client or test fake).
package bootstrap

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// Entry is the routing-relevant slice of a filer entry. SuccessorModTime
// and NoncurrentIndex are populated only on versioned-bucket walks; the
// retention path bails out conservatively when they're zero / nil.
//
// MPU init directories at .uploads/<id> populate DestKey with the
// destination object key (from entry.Extended[ExtMultipartObjectKey]) so
// rule-prefix matching works on the user's intended path while Path
// stays as the upload directory the dispatcher must rm.
type Entry struct {
	Path           string
	DestKey        string
	ModTime        time.Time
	Size           int64
	IsDirectory    bool
	IsLatest       bool
	IsDeleteMarker bool
	IsMPUInit      bool
	NumVersions    int
	Tags           map[string]string

	SuccessorModTime time.Time
	NoncurrentIndex  *int

	// VersionID is the S3 version id of this entry, empty for
	// non-versioned buckets. Populated by the ListFunc adapter when
	// the entry came from a `.versions/` directory; the walker itself
	// doesn't use it, but the Dispatcher needs it to address the
	// right version on LifecycleDelete.
	VersionID string
}

// ListFunc must skip entries with Path <= start so kill-resume picks up
// where the previous run stopped.
type ListFunc func(ctx context.Context, bucket, start string, cb func(*Entry) error) error

// Dispatcher executes one (action, entry) verdict. An error halts the walk;
// the caller decides whether to retry from the recorded last_scanned_path.
type Dispatcher interface {
	Delete(ctx context.Context, action *engine.CompiledAction, entry *Entry) error
	// Annotate persists the computed expiration date for a not-yet-due object so
	// GET/HEAD handlers can return the x-amz-expiration response header without
	// re-evaluating lifecycle rules on every request. Errors are non-fatal: a
	// missing annotation is a missing header, not a data-loss event.
	Annotate(ctx context.Context, bucket string, entry *Entry, expiresAt time.Time, ruleID string) error
}

// Checkpoint is the resume state. Caller persists it under
// /etc/s3/lifecycle/<bucket>/_bootstrap.
type Checkpoint struct {
	LastScannedPath string
	Completed       bool
}

type WalkOptions struct {
	Resume string
	Now    time.Time
}

// Walk iterates entries via list, evaluates each active ActionKey via
// MatchPath + EvaluateAction, and calls Dispatcher.Delete for currently-due
// actions. SCAN_AT_DATE actions are skipped (their bootstrap is scheduled
// separately).
func Walk(ctx context.Context, snap *engine.Snapshot, bucket string, list ListFunc, dispatch Dispatcher, opts WalkOptions) (Checkpoint, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cp := Checkpoint{LastScannedPath: opts.Resume}

	// Reuse one ObjectInfo across the walk; EvaluateAction reads it
	// synchronously without retaining.
	var info s3lifecycle.ObjectInfo

	err := list(ctx, bucket, opts.Resume, func(entry *Entry) error {
		if entry == nil || entry.Path == "" {
			return nil
		}
		// MPU init directories at .uploads/<id> are the one directory
		// shape lifecycle cares about; everything else stays out.
		if entry.IsDirectory && !entry.IsMPUInit {
			cp.LastScannedPath = entry.Path
			return nil
		}
		if err := walkEntry(ctx, snap, bucket, entry, dispatch, now, &info); err != nil {
			return err
		}
		cp.LastScannedPath = entry.Path
		return nil
	})
	if err != nil {
		return cp, err
	}
	cp.Completed = true
	return cp, nil
}

func walkEntry(ctx context.Context, snap *engine.Snapshot, bucket string, entry *Entry, dispatch Dispatcher, now time.Time, info *s3lifecycle.ObjectInfo) error {
	// MPU init: rule-prefix matching uses the destination key, not the
	// .uploads/<id> directory path. A bare directory with no DestKey is
	// either a stray dir or an init mid-write before metadata landed —
	// skip rather than guess.
	matchKey := entry.Path
	if entry.IsMPUInit {
		if entry.DestKey == "" {
			return nil
		}
		matchKey = entry.DestKey
	}
	keys := snap.MatchPath(bucket, matchKey, nil)
	if len(keys) == 0 {
		return nil
	}
	*info = s3lifecycle.ObjectInfo{
		Key:              matchKey,
		ModTime:          entry.ModTime,
		Size:             entry.Size,
		IsLatest:         entry.IsLatest,
		IsDeleteMarker:   entry.IsDeleteMarker,
		IsMPUInit:        entry.IsMPUInit,
		NumVersions:      entry.NumVersions,
		SuccessorModTime: entry.SuccessorModTime,
		NoncurrentIndex:  entry.NoncurrentIndex,
		Tags:             entry.Tags,
	}
	// earliestExpiry tracks the soonest expiration across all matching expiration-kind
	// rules that are not yet due. Used to annotate the object after the loop.
	var earliestExpiry time.Time
	var expiryRuleID string
	dispatched := false

	for _, key := range keys {
		action := snap.Action(key)
		if action == nil {
			continue
		}
		// DISABLED can be flipped at runtime independent of XML Status,
		// so skip it even though EvaluateAction would also reject.
		// SCAN_AT_DATE actions are processed here too — the date check
		// in EvaluateAction (now.Before(rule.ExpirationDate)) gates the
		// dispatch, so pre-date walks are no-ops and post-date walks
		// expire eligible objects. The earlier "scan-at-date runs its
		// own bootstrap" plan was never wired; until that lands, the
		// regular bootstrap walk is the only path that fires
		// ExpirationDate rules.
		if action.Mode == engine.ModeDisabled {
			continue
		}
		// (kind, info) shape gate: ABORT_MPU only on MPU init records,
		// every other kind only on regular objects/versions. Mismatched
		// pairs would either dispatch a noncurrent action with empty
		// version_id (server BLOCKs, cursor freezes) or dispatch
		// ABORT_MPU against a regular object path.
		if entry.IsMPUInit && key.ActionKind != s3lifecycle.ActionKindAbortMPU {
			continue
		}
		if !entry.IsMPUInit && key.ActionKind == s3lifecycle.ActionKindAbortMPU {
			continue
		}
		res := s3lifecycle.EvaluateAction(action.Rule, key.ActionKind, info, now)
		if res.Action == s3lifecycle.ActionNone {
			// Not yet due: capture the earliest expiration date so we can annotate.
			if key.ActionKind == s3lifecycle.ActionKindExpirationDays ||
				key.ActionKind == s3lifecycle.ActionKindExpirationDate {
				dueAt := s3lifecycle.ComputeDueAt(action.Rule, key.ActionKind, info)
				if !dueAt.IsZero() && (earliestExpiry.IsZero() || dueAt.Before(earliestExpiry)) {
					earliestExpiry = dueAt
					expiryRuleID = action.Rule.ID
				}
			}
			continue
		}
		if err := dispatch.Delete(ctx, action, entry); err != nil {
			glog.Warningf("lifecycle bootstrap: dispatch %s/%s kind=%s: %v",
				bucket, entry.Path, key.ActionKind, err)
			return err
		}
		dispatched = true
		stats.S3LifecycleBootstrapDispatchCounter.WithLabelValues(bucket, key.ActionKind.String()).Inc()
	}

	// Annotate not-yet-expired objects so GET/HEAD can return x-amz-expiration.
	// Skip if the object was already dispatched for deletion.
	if !dispatched && !earliestExpiry.IsZero() {
		if err := dispatch.Annotate(ctx, bucket, entry, earliestExpiry, expiryRuleID); err != nil {
			glog.Warningf("lifecycle bootstrap: annotate %s/%s: %v", bucket, entry.Path, err)
			// Non-fatal: a missing annotation is just a missing response header.
		}
	}
	return nil
}

// EntryCallback wraps an in-memory slice as a ListFunc; useful for tests.
func EntryCallback(entries []*Entry) ListFunc {
	return func(ctx context.Context, bucket, start string, cb func(*Entry) error) error {
		for _, e := range entries {
			if start != "" && e.Path <= start {
				continue
			}
			if err := cb(e); err != nil {
				return err
			}
		}
		return nil
	}
}

func HasPrefix(path, prefix string) bool { return strings.HasPrefix(path, prefix) }
