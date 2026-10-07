package goreservationwaitlist

import (
	"sync"
	"testing"
	"time"
)

func joinProfile(t *testing.T, w *Waitlist, id string, p AppProfile, now time.Time) {
	t.Helper()
	if _, err := w.JoinWithProfile(id, p, now); err != nil {
		t.Fatalf("JoinWithProfile(%s): %v", id, err)
	}
}

func queueIDs(w *Waitlist) []string {
	entries := w.QueueView()
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.Application.ID
	}
	return ids
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestJoinSnapshotAndStableFIFO(t *testing.T) {
	w := New(3, time.Minute)
	joinProfile(t, w, "a", AppProfile{Eligible: true, Priority: 1}, base)
	joinProfile(t, w, "b", AppProfile{Eligible: false, Priority: 9}, base.Add(time.Second))
	joinProfile(t, w, "c", AppProfile{Eligible: true, Priority: 5}, base.Add(2*time.Second))

	entries := w.QueueView()
	if !sameStrings(queueIDs(w), []string{"a", "b", "c"}) {
		t.Fatalf("queue = %v, want FIFO [a b c]", queueIDs(w))
	}
	if entries[2].Application.Profile.Priority != 5 || entries[2].Application.RuleVersion != 1 {
		t.Fatalf("snapshot not frozen: %+v", entries[2].Application)
	}
	if entries[0].Application.Seq == entries[1].Application.Seq {
		t.Fatalf("enqueue seq must be unique")
	}
}

func TestReorderPreviewChangesAndReasons(t *testing.T) {
	w := New(3, time.Minute)
	joinProfile(t, w, "a", AppProfile{Eligible: false, Priority: 1}, base)
	joinProfile(t, w, "b", AppProfile{Eligible: true, Priority: 1}, base)
	joinProfile(t, w, "c", AppProfile{Eligible: true, Priority: 9}, base)

	preview, err := w.PrepareReorder(ReorderRequest{
		ID:         "reorder-1",
		Conditions: []Condition{{Field: FieldEligible}, {Field: FieldPriority}},
		Note:       "eligible then priority",
	}, base)
	if err != nil {
		t.Fatalf("PrepareReorder: %v", err)
	}
	if !sameStrings(preview.Order, []string{"c", "b", "a"}) {
		t.Fatalf("preview order = %v, want [c b a]", preview.Order)
	}
	if preview.BaseQueueVersion != w.View().QueueVersion {
		t.Fatalf("baseline queue version mismatch")
	}
	if preview.Rule.Version <= 1 || preview.BaseRuleVersion != 1 {
		t.Fatalf("rule versions = new %d base %d", preview.Rule.Version, preview.BaseRuleVersion)
	}
	changeByID := map[string]PositionChange{}
	for _, ch := range preview.Changes {
		changeByID[ch.ApplicationID] = ch
	}
	c, ok := changeByID["c"]
	if !ok || c.From != 3 || c.To != 1 {
		t.Fatalf("change for c = %+v", c)
	}
	if c.Reason == "" || c.Reason[:8] != "eligible" {
		t.Fatalf("c reason = %q, want eligible-based", c.Reason)
	}
	if !sameStrings(queueIDs(w), []string{"a", "b", "c"}) {
		t.Fatalf("queue changed before publish: %v", queueIDs(w))
	}
}

func TestReorderPublishAtomicAndView(t *testing.T) {
	w := New(3, time.Minute)
	joinProfile(t, w, "a", AppProfile{Priority: 1}, base)
	joinProfile(t, w, "b", AppProfile{Priority: 5}, base)
	joinProfile(t, w, "c", AppProfile{Priority: 3}, base)

	preview, err := w.PrepareReorder(ReorderRequest{
		ID:         "reorder-1",
		Conditions: []Condition{{Field: FieldPriority}},
	}, base)
	if err != nil {
		t.Fatal(err)
	}
	qvBefore := w.View().QueueVersion
	published, err := w.PublishReorder("reorder-1", base)
	if err != nil {
		t.Fatalf("PublishReorder: %v", err)
	}
	if !sameStrings(published.Order, preview.Order) || !sameStrings(queueIDs(w), []string{"b", "c", "a"}) {
		t.Fatalf("published order = %v, live = %v", published.Order, queueIDs(w))
	}
	view := w.View()
	if view.QueueVersion != qvBefore+1 || view.RuleVersion != preview.Rule.Version {
		t.Fatalf("view versions = qv %d rule %d, want %d/%d",
			view.QueueVersion, view.RuleVersion, qvBefore+1, preview.Rule.Version)
	}
	if len(view.Changes) != len(preview.Changes) {
		t.Fatalf("view changes = %+v, want %+v", view.Changes, preview.Changes)
	}
	if _, err := w.PublishReorder("reorder-1", base); err != ErrPreviewGone {
		t.Fatalf("republish err = %v, want ErrPreviewGone", err)
	}
	joinProfile(t, w, "d", AppProfile{Priority: 4}, base)
	if !sameStrings(queueIDs(w), []string{"b", "d", "c", "a"}) {
		t.Fatalf("queue after join = %v, want [b d c a]", queueIDs(w))
	}
}

func TestReorderKeepsReservedOffersIntact(t *testing.T) {
	w := New(1, time.Minute)
	joinProfile(t, w, "low", AppProfile{Priority: 1}, base)
	joinProfile(t, w, "high", AppProfile{Priority: 9}, base)

	offer := mustPromote(t, w, "promo-1", base.Add(time.Minute))
	preview, err := w.PrepareReorder(ReorderRequest{
		ID:         "reorder-1",
		Conditions: []Condition{{Field: FieldPriority}},
	}, base)
	if err != nil {
		t.Fatal(err)
	}
	if !sameStrings(preview.Order, []string{"high"}) {
		t.Fatalf("preview = %v, must only contain not-yet-promoted [high]", preview.Order)
	}
	if _, err := w.PublishReorder("reorder-1", base); err != nil {
		t.Fatalf("publish: %v", err)
	}
	view := w.View()
	if view.Slots[0].State != SlotReserved || view.Slots[0].PromotionID != offer.ID {
		t.Fatalf("slot = %+v, reservation silently revoked", view.Slots[0])
	}
	if view.Promotions[0].ApplicationID != "low" || view.Promotions[0].State != PromotionPending {
		t.Fatalf("promotion = %+v, want pending offer to low", view.Promotions[0])
	}
	got, err := w.Confirm(offer.ID, base)
	if err != nil {
		t.Fatalf("Confirm original offer after reorder: %v", err)
	}
	if got.State != PromotionConfirmed {
		t.Fatalf("state = %s", got.State)
	}
	if !sameStrings(queueIDs(w), []string{"high"}) {
		t.Fatalf("queue = %v, want [high]", queueIDs(w))
	}
}

func TestPreviewInvalidatedByBaselineChanges(t *testing.T) {
	cases := []struct {
		name string
		mut  func(w *Waitlist)
	}{
		{"join", func(w *Waitlist) {
			joinProfile(t, w, "late", AppProfile{Priority: 0}, base)
		}},
		{"leave", func(w *Waitlist) {
			if err := w.Leave("c"); err != nil {
				t.Fatal(err)
			}
		}},
		{"promote", func(w *Waitlist) {
			mustPromote(t, w, "promo-9", base.Add(time.Minute))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := New(2, time.Minute)
			joinProfile(t, w, "a", AppProfile{Priority: 1}, base)
			joinProfile(t, w, "b", AppProfile{Priority: 2}, base)
			joinProfile(t, w, "c", AppProfile{Priority: 3}, base)
			if _, err := w.PrepareReorder(ReorderRequest{
				ID:         "reorder-1",
				Conditions: []Condition{{Field: FieldPriority}},
			}, base); err != nil {
				t.Fatal(err)
			}
			tc.mut(w)
			if _, err := w.PublishReorder("reorder-1", base); err != ErrPreviewGone {
				t.Fatalf("publish after %s err = %v, want ErrPreviewGone", tc.name, err)
			}
			if _, err := w.Preview("reorder-1"); err != ErrPreviewGone {
				t.Fatalf("preview lookup after %s err = %v, want ErrPreviewGone", tc.name, err)
			}
		})
	}
}

func TestPreviewDeclineReleaseAlsoInvalidates(t *testing.T) {
	w := New(1, time.Minute)
	joinProfile(t, w, "a", AppProfile{Priority: 1}, base)
	joinProfile(t, w, "b", AppProfile{Priority: 2}, base)
	offer := mustPromote(t, w, "promo-1", base.Add(time.Minute))
	if _, err := w.PrepareReorder(ReorderRequest{
		ID:         "reorder-1",
		Conditions: []Condition{{Field: FieldPriority}},
	}, base); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Decline(offer.ID, base); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PublishReorder("reorder-1", base); err != ErrPreviewGone {
		t.Fatalf("publish after decline err = %v, want ErrPreviewGone", err)
	}
}

func TestReorderIdempotentReplayConflictAndStale(t *testing.T) {
	w := New(3, time.Minute)
	joinProfile(t, w, "a", AppProfile{Priority: 1}, base)
	joinProfile(t, w, "b", AppProfile{Priority: 2}, base)
	req := ReorderRequest{ID: "reorder-1", Conditions: []Condition{{Field: FieldPriority}}}

	first, err := w.PrepareReorder(req, base)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := w.PrepareReorder(req, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay.ID != first.ID || replay.Rule.Version != first.Rule.Version ||
		!sameStrings(replay.Order, first.Order) {
		t.Fatalf("replay = %+v, want %+v", replay, first)
	}

	changed := req
	changed.Conditions = []Condition{{Field: FieldPriority}, {Field: FieldEligible}}
	if _, err := w.PrepareReorder(changed, base); err != ErrConflict {
		t.Fatalf("changed conditions err = %v, want ErrConflict", err)
	}

	joinProfile(t, w, "c", AppProfile{Priority: 3}, base)
	if _, err := w.PrepareReorder(req, base); err != ErrStaleVersion {
		t.Fatalf("replay after baseline drift err = %v, want ErrStaleVersion", err)
	}
}

func TestScanUsesPublishedOrderAndNeverRepromotes(t *testing.T) {
	w := New(1, time.Minute)
	joinProfile(t, w, "a", AppProfile{Priority: 1}, base)
	joinProfile(t, w, "b", AppProfile{Priority: 9}, base)
	joinProfile(t, w, "c", AppProfile{Priority: 5}, base)
	offer := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	if _, err := w.PrepareReorder(ReorderRequest{
		ID:         "reorder-1",
		Conditions: []Condition{{Field: FieldPriority}},
	}, base); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PublishReorder("reorder-1", base); err != nil {
		t.Fatal(err)
	}
	if !sameStrings(queueIDs(w), []string{"b", "c"}) {
		t.Fatalf("queue = %v, want [b c]", queueIDs(w))
	}
	res := w.Scan(base.Add(2 * time.Minute))
	if len(res.Expired) != 1 || res.Expired[0].ID != offer.ID {
		t.Fatalf("expired = %+v", res.Expired)
	}
	if len(res.Promoted) != 1 || res.Promoted[0].ApplicationID != "b" {
		t.Fatalf("promoted = %+v, want b under new order", res.Promoted)
	}
	promo := res.Promoted[0]
	if promo.RuleVersion != w.View().RuleVersion {
		t.Fatalf("promo rule = %d, want active %d", promo.RuleVersion, w.View().RuleVersion)
	}
	if promo.Basis.RuleVersion != w.View().RuleVersion ||
		len(promo.Basis.Reasons) == 0 || promo.Basis.Position != 1 {
		t.Fatalf("promotion basis = %+v", promo.Basis)
	}
	res = w.Scan(base.Add(3 * time.Minute))
	for _, p := range res.Promoted {
		if p.ApplicationID == "b" || p.ApplicationID == "a" {
			t.Fatalf("already-processed app re-promoted: %+v", p)
		}
	}
}

func TestConcurrentScanLeavePublishSingleVersion(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		w := New(1, time.Minute)
		for i := 0; i < 6; i++ {
			joinProfile(t, w, string(rune('a'+i)), AppProfile{Priority: i}, base)
		}
		offer := mustPromote(t, w, "promo-0", base.Add(time.Minute))

		prepare := func(id string) {
			_, _ = w.PrepareReorder(ReorderRequest{
				ID:         id,
				Conditions: []Condition{{Field: FieldPriority}},
			}, base)
		}

		var wg sync.WaitGroup
		ops := []func(){
			func() { prepare("r-1"); _, _ = w.PublishReorder("r-1", base) },
			func() { w.Scan(base.Add(2 * time.Minute)) },
			func() { _ = w.Leave("c") },
			func() { _, _ = w.Decline(offer.ID, base) },
			func() {
				prepare("r-2")
				_, _ = w.PublishReorder("r-2", base)
			},
		}
		for _, op := range ops {
			wg.Add(1)
			go func(op func()) { defer wg.Done(); op() }(op)
		}
		wg.Wait()

		view := w.View()
		seen := map[string]bool{}
		for _, e := range view.Queue {
			if seen[e.ID] {
				t.Fatalf("iter %d: duplicate %s in queue", iter, e.ID)
			}
			seen[e.ID] = true
			if e.Status != StatusWaiting {
				t.Fatalf("iter %d: non-waiting %s in queue (%s)", iter, e.ID, e.Status)
			}
		}
		promoted := map[string]bool{}
		for _, p := range view.Promotions {
			if promoted[p.ID] {
				t.Fatalf("iter %d: promotion %s listed twice", iter, p.ID)
			}
			promoted[p.ID] = true
		}
		for _, s := range view.Slots {
			if s.State == SlotReserved {
				if !promoted[s.PromotionID] {
					t.Fatalf("iter %d: slot reserved by unknown promotion", iter)
				}
			}
		}
	}
}

func TestStalePublishAppliesNothing(t *testing.T) {
	w := New(3, time.Minute)
	joinProfile(t, w, "a", AppProfile{Priority: 1}, base)
	joinProfile(t, w, "b", AppProfile{Priority: 5}, base)
	joinProfile(t, w, "c", AppProfile{Priority: 3}, base)
	if _, err := w.PrepareReorder(ReorderRequest{
		ID:         "reorder-1",
		Conditions: []Condition{{Field: FieldPriority}},
	}, base); err != nil {
		t.Fatal(err)
	}
	// Baseline moves before confirmation.
	joinProfile(t, w, "d", AppProfile{Priority: 9}, base)
	if _, err := w.PublishReorder("reorder-1", base); err != ErrPreviewGone {
		t.Fatalf("publish err = %v, want ErrPreviewGone", err)
	}
	// The stale ordering must not have been applied, not even partially.
	if !sameStrings(queueIDs(w), []string{"a", "b", "c", "d"}) {
		t.Fatalf("queue = %v, stale preview must not apply at all", queueIDs(w))
	}
	if w.View().RuleVersion != 1 {
		t.Fatalf("active rule changed on failed publish")
	}

	// Re-preparing on the new baseline and publishing works atomically.
	preview, err := w.PrepareReorder(ReorderRequest{
		ID:         "reorder-2",
		Conditions: []Condition{{Field: FieldPriority}},
	}, base)
	if err != nil {
		t.Fatal(err)
	}
	if !sameStrings(preview.Order, []string{"d", "b", "c", "a"}) {
		t.Fatalf("fresh preview = %v, want [d b c a]", preview.Order)
	}
	if _, err := w.PublishReorder("reorder-2", base); err != nil {
		t.Fatalf("publish fresh: %v", err)
	}
	if !sameStrings(queueIDs(w), []string{"d", "b", "c", "a"}) {
		t.Fatalf("queue = %v, want [d b c a]", queueIDs(w))
	}
}

func TestRuleVersionsAreImmutable(t *testing.T) {
	w := New(3, time.Minute)
	r1 := w.ActiveRule()
	r2, err := w.InstallRules([]Condition{{Field: FieldPriority}}, "priority", base)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Version <= r1.Version {
		t.Fatalf("new version %d not greater than %d", r2.Version, r1.Version)
	}
	again, err := w.InstallRules([]Condition{{Field: FieldPriority}}, "priority copy", base)
	if err != nil {
		t.Fatal(err)
	}
	if again.Version == r2.Version {
		t.Fatalf("identical conditions must still get a fresh version")
	}
	got1, err := w.Rule(r1.Version)
	if err != nil {
		t.Fatal(err)
	}
	if len(got1.Conditions) != 1 || got1.Conditions[0].Field != FieldSeq {
		t.Fatalf("old rule mutated: %+v", got1)
	}
	if w.ActiveRule().Version != r1.Version {
		t.Fatalf("active rule changed without a published reorder")
	}
	if _, err := w.Rule(999); err != ErrRuleMissing {
		t.Fatalf("missing rule err = %v, want ErrRuleMissing", err)
	}
}
