package goreservationwaitlist

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func mustJoinWith(t *testing.T, w *Waitlist, id string, q Qualification, at time.Time) Application {
	t.Helper()
	app, err := w.JoinWith(id, q, at)
	if err != nil {
		t.Fatalf("JoinWith(%s): %v", id, err)
	}
	return app
}

func queueIDs(v View) []string {
	ids := make([]string, len(v.Queue))
	for i, a := range v.Queue {
		ids[i] = a.ID
	}
	return ids
}

func tierThenTime() Rule {
	return Rule{Version: 2, Criteria: []CriterionID{CriterionTier, CriterionWaitTime}}
}

func hasBasisEntry(basis []string, key string) bool {
	for _, b := range basis {
		if strings.HasPrefix(b, key+"=") {
			return true
		}
	}
	return false
}

func TestJoinSnapshotBasisUnderRuleVersion(t *testing.T) {
	w := New(2, time.Minute)
	app := mustJoinWith(t, w, "app-1", Qualification{Tier: 3, Score: 10}, base)
	if app.RuleVersion != 1 {
		t.Fatalf("RuleVersion = %d, want 1", app.RuleVersion)
	}
	if !hasBasisEntry(app.Basis, "wait-time") || !hasBasisEntry(app.Basis, "seq") {
		t.Fatalf("Basis = %v, want wait-time + seq tie-break", app.Basis)
	}

	mustJoinWith(t, w, "app-2", Qualification{Tier: 9}, base.Add(time.Second))
	if got := queueIDs(w.View()); got[0] != "app-1" || got[1] != "app-2" {
		t.Fatalf("queue = %v, want FIFO under v1", got)
	}

	w2 := New(2, time.Minute)
	mustJoinWith(t, w2, "b", Qualification{}, base)
	mustJoinWith(t, w2, "a", Qualification{}, base)
	if got := queueIDs(w2.View()); got[0] != "b" || got[1] != "a" {
		t.Fatalf("tie order = %v, want insertion order", got)
	}
}

func TestPreviewReorderShowsPositionsAndReasons(t *testing.T) {
	w := New(2, time.Minute)
	mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
	mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base.Add(time.Second))

	rule := tierThenTime()
	preview, err := w.PreviewReorder(ReorderRequest{ID: "reorder-1", Rule: rule})
	if err != nil {
		t.Fatalf("PreviewReorder: %v", err)
	}
	if preview.BaselineVersion != w.View().Version {
		t.Fatalf("baseline = %d, want %d", preview.BaselineVersion, w.View().Version)
	}
	if len(preview.Order) != 2 || preview.Order[0] != "app-2" {
		t.Fatalf("preview order = %v, want app-2 first by tier", preview.Order)
	}
	if got := queueIDs(w.View()); got[0] != "app-1" {
		t.Fatalf("queue mutated before commit: %v", got)
	}
	byApp := map[string]PositionChange{}
	for _, c := range preview.Changes {
		byApp[c.ApplicationID] = c
	}
	up, ok := byApp["app-2"]
	if !ok || up.From != 1 || up.To != 0 {
		t.Fatalf("app-2 change = %+v, want 1 -> 0", up)
	}
	if !strings.Contains(up.Reason, string(CriterionTier)) {
		t.Fatalf("app-2 reason = %q, want tier criterion cited", up.Reason)
	}
	down := byApp["app-1"]
	if down.From != 0 || down.To != 1 {
		t.Fatalf("app-1 change = %+v, want 0 -> 1", down)
	}
}

func TestPreviewReplaySameRuleAndBaseline(t *testing.T) {
	w := New(2, time.Minute)
	mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
	mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base)
	req := ReorderRequest{ID: "reorder-1", Rule: tierThenTime()}
	first, err := w.PreviewReorder(req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := w.PreviewReorder(req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if again.BaselineVersion != first.BaselineVersion ||
		len(again.Order) != len(first.Order) || again.Order[0] != first.Order[0] {
		t.Fatalf("replay = %+v, want %+v", again, first)
	}

	req.Rule = Rule{Version: 3, Criteria: []CriterionID{CriterionScore}}
	if _, err := w.PreviewReorder(req); err != ErrConflict {
		t.Fatalf("changed rule err = %v, want ErrConflict", err)
	}
}

func TestPreviewStaleAfterBaselineChanges(t *testing.T) {
	cases := []struct {
		name string
		mut  func(w *Waitlist)
	}{
		{"join", func(w *Waitlist) { mustJoinWith(t, w, "app-3", Qualification{}, base) }},
		{"leave", func(w *Waitlist) {
			if err := w.Leave("app-1"); err != nil {
				t.Fatal(err)
			}
		}},
		{"promote", func(w *Waitlist) {
			if _, err := w.Promote(PromoteRequest{ID: "promo-x", NotificationID: "n", Deadline: base.Add(time.Minute)}, base); err != nil {
				t.Fatal(err)
			}
		}},
		{"qualification", func(w *Waitlist) {
			if _, err := w.UpdateQualification("app-1", Qualification{Tier: 9}); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := New(3, time.Minute)
			mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
			mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base)
			req := ReorderRequest{ID: "reorder-1", Rule: tierThenTime()}
			if _, err := w.PreviewReorder(req); err != nil {
				t.Fatal(err)
			}
			tc.mut(w)
			if _, err := w.CommitReorder("reorder-1"); err != ErrStaleVersion {
				t.Fatalf("commit after %s err = %v, want ErrStaleVersion", tc.name, err)
			}
			if _, err := w.PreviewReorder(req); err != ErrStaleVersion {
				t.Fatalf("replay after %s err = %v, want ErrStaleVersion", tc.name, err)
			}
		})
	}
}

func TestCommitReorderPublishesNewQueueVersion(t *testing.T) {
	w := New(2, time.Minute)
	mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
	mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base)
	before := w.View().Version

	rule := tierThenTime()
	if _, err := w.PreviewReorder(ReorderRequest{ID: "reorder-1", Rule: rule}); err != nil {
		t.Fatal(err)
	}
	res, err := w.CommitReorder("reorder-1")
	if err != nil {
		t.Fatalf("CommitReorder: %v", err)
	}
	if res.NewVersion <= before {
		t.Fatalf("NewVersion = %d, want > %d", res.NewVersion, before)
	}
	v := w.View()
	if !v.Rule.equal(rule) || v.Version != res.NewVersion {
		t.Fatalf("view rule/version = %+v/%d, want %+v/%d", v.Rule, v.Version, rule, res.NewVersion)
	}
	if got := queueIDs(v); got[0] != "app-2" || got[1] != "app-1" {
		t.Fatalf("queue after commit = %v", got)
	}
	for _, a := range v.Queue {
		if a.RuleVersion != rule.Version || !hasBasisEntry(a.Basis, "tier") {
			t.Fatalf("%s snapshot = rule %d basis %v", a.ID, a.RuleVersion, a.Basis)
		}
	}
	again, err := w.CommitReorder("reorder-1")
	if err != nil {
		t.Fatalf("commit replay: %v", err)
	}
	if again.NewVersion != res.NewVersion {
		t.Fatalf("replay version = %d, want %d", again.NewVersion, res.NewVersion)
	}
}

func TestPendingOfferKeptAcrossReorder(t *testing.T) {
	w := New(1, time.Minute)
	mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
	mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base)
	p := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	if _, err := w.PreviewReorder(ReorderRequest{ID: "r", Rule: tierThenTime()}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.CommitReorder("r"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	v := w.View()
	if v.Slots[0].State != SlotReserved || v.Slots[0].PromotionID != p.ID {
		t.Fatalf("slot = %+v, want still reserved by %s", v.Slots[0], p.ID)
	}
	if len(v.Queue) != 1 || v.Queue[0].ID != "app-2" {
		t.Fatalf("waiting queue = %v, want only app-2", queueIDs(v))
	}
	got, err := w.Confirm(p.ID, base)
	if err != nil {
		t.Fatalf("Confirm after reorder: %v", err)
	}
	if got.State != PromotionConfirmed || got.ApplicationID != "app-1" {
		t.Fatalf("promotion = %+v, want confirmed for app-1", got)
	}
	if v2 := w.View(); v2.Slots[0].State != SlotOccupied {
		t.Fatalf("slot state = %s, want occupied by original offer", v2.Slots[0].State)
	}
}

func TestPromotionRecordsBasisAndScanUsesNewOrder(t *testing.T) {
	w := New(1, time.Minute)
	mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
	mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base)
	if _, err := w.PreviewReorder(ReorderRequest{ID: "r", Rule: tierThenTime()}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.CommitReorder("r"); err != nil {
		t.Fatal(err)
	}
	p, err := w.Promote(PromoteRequest{ID: "promo-new", NotificationID: "n", Deadline: base.Add(time.Minute)}, base)
	if err != nil {
		t.Fatal(err)
	}
	if p.ApplicationID != "app-2" || p.RuleVersion != 2 || !hasBasisEntry(p.Basis, "tier") {
		t.Fatalf("promotion = %+v, want app-2 under rule v2 with tier basis", p)
	}

	if _, err := w.Decline(p.ID, base); err != nil {
		t.Fatal(err)
	}
	res := w.Scan(base)
	if len(res.Promoted) != 1 || res.Promoted[0].ApplicationID != "app-1" {
		t.Fatalf("scan promoted = %+v, want app-1 only", res.Promoted)
	}
}

func TestPromoteBaseVersionPin(t *testing.T) {
	w := New(2, time.Minute)
	mustJoinWith(t, w, "app-1", Qualification{}, base)
	mustJoinWith(t, w, "app-2", Qualification{}, base)
	stale := w.View().Version
	mustJoinWith(t, w, "app-3", Qualification{}, base)

	if _, err := w.Promote(PromoteRequest{
		ID: "p", NotificationID: "n", Deadline: base.Add(time.Minute), BaseVersion: stale,
	}, base); err != ErrStaleVersion {
		t.Fatalf("pinned Promote err = %v, want ErrStaleVersion", err)
	}
	current := w.View().Version
	p, err := w.Promote(PromoteRequest{
		ID: "p", NotificationID: "n", Deadline: base.Add(time.Minute), BaseVersion: current,
	}, base)
	if err != nil {
		t.Fatalf("current-version Promote: %v", err)
	}
	if p.ApplicationID != "app-1" {
		t.Fatalf("promoted %s, want app-1", p.ApplicationID)
	}
}

func TestViewExposesRuleChangesAndBasis(t *testing.T) {
	w := New(2, time.Minute)
	mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
	mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base)
	if _, err := w.PreviewReorder(ReorderRequest{ID: "r", Rule: tierThenTime()}); err != nil {
		t.Fatal(err)
	}
	v := w.View()
	if v.OutstandingPreview == nil || v.OutstandingPreview.ID != "r" {
		t.Fatalf("outstanding preview = %+v", v.OutstandingPreview)
	}
	if _, err := w.CommitReorder("r"); err != nil {
		t.Fatal(err)
	}
	v = w.View()
	if v.LastReorder == nil || v.LastReorder.ID != "r" || len(v.LastReorder.Changes) == 0 {
		t.Fatalf("last reorder = %+v", v.LastReorder)
	}
	if v.OutstandingPreview != nil {
		t.Fatalf("outstanding preview = %+v, want nil after commit", v.OutstandingPreview)
	}
}

func TestConcurrentScanPromoteReorderSingleVersion(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		w := New(1, time.Minute)
		mustJoinWith(t, w, "app-1", Qualification{Tier: 1}, base)
		mustJoinWith(t, w, "app-2", Qualification{Tier: 2}, base)

		var wg sync.WaitGroup
		errs := make(chan error, 4)
		run := func(fn func()) {
			wg.Add(1)
			go func() { defer wg.Done(); fn() }()
		}
		run(func() {
			_, err := w.PreviewReorder(ReorderRequest{ID: "r", Rule: tierThenTime()})
			errs <- err
		})
		run(func() {
			w.Scan(base)
			errs <- nil
		})
		run(func() {
			_ = w.Leave("app-2")
			errs <- nil
		})
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("iter %d: concurrent op: %v", iter, err)
			}
		}

		// Commit must be atomic against the baseline: either it was made
		// before any mutation and succeeds, or it fails stale wholesale.
		_, commitErr := w.CommitReorder("r")
		v := w.View()
		if commitErr == nil {
			if !v.Rule.equal(tierThenTime()) {
				t.Fatalf("iter %d: committed but rule = %+v", iter, v.Rule)
			}
		} else if commitErr != ErrStaleVersion && commitErr != ErrPreviewNotFound {
			t.Fatalf("iter %d: commit err = %v", iter, commitErr)
		}

		// Exactly one application may own a promotion for the slot, and no
		// application is promoted twice.
		seen := map[string]int{}
		for _, p := range v.Promotions {
			seen[p.ApplicationID]++
		}
		for appID, n := range seen {
			if n > 1 {
				t.Fatalf("iter %d: %s promoted %d times", iter, appID, n)
			}
		}
		for _, s := range v.Slots {
			if s.State == SlotReserved || s.State == SlotOccupied {
				if s.PromotionID == "" {
					t.Fatalf("iter %d: taken slot without promotion", iter)
				}
			}
		}
	}
}
