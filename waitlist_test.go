package goreservationwaitlist

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func mustJoin(t *testing.T, w *Waitlist, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := w.Join(id, base); err != nil {
			t.Fatalf("Join(%s): %v", id, err)
		}
	}
}

func mustPromote(t *testing.T, w *Waitlist, id string, deadline time.Time) Promotion {
	t.Helper()
	p, err := w.Promote(PromoteRequest{ID: id, NotificationID: "notif-" + id, Deadline: deadline}, base)
	if err != nil {
		t.Fatalf("Promote(%s): %v", id, err)
	}
	return p
}

func TestConfirmAtDeadlineBoundary(t *testing.T) {
	w := New(1, time.Minute)
	mustJoin(t, w, "app-1")
	p := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	// Confirm exactly at the deadline is still valid.
	got, err := w.Confirm(p.ID, p.Deadline)
	if err != nil {
		t.Fatalf("Confirm at deadline: %v", err)
	}
	if got.State != PromotionConfirmed {
		t.Fatalf("state = %s, want confirmed", got.State)
	}
	view := w.View()
	if view.Slots[0].State != SlotOccupied {
		t.Fatalf("slot state = %s, want occupied", view.Slots[0].State)
	}
}

func TestConfirmAfterDeadlineExpires(t *testing.T) {
	w := New(1, time.Minute)
	mustJoin(t, w, "app-1")
	p := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	if _, err := w.Confirm(p.ID, p.Deadline.Add(time.Nanosecond)); err != ErrExpired {
		t.Fatalf("Confirm past deadline err = %v, want ErrExpired", err)
	}
	view := w.View()
	if view.Slots[0].State != SlotFree {
		t.Fatalf("slot state = %s, want free after expiry", view.Slots[0].State)
	}
	if view.Promotions[0].State != PromotionExpired {
		t.Fatalf("promotion state = %s, want expired", view.Promotions[0].State)
	}
}

func TestConfirmStaleVersionRejected(t *testing.T) {
	w := New(2, time.Minute)
	mustJoin(t, w, "app-1", "app-2")
	p := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	// Any waitlist change after the offer invalidates it.
	if _, err := w.Join("app-3", base); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Confirm(p.ID, base); err != ErrStaleVersion {
		t.Fatalf("Confirm err = %v, want ErrStaleVersion", err)
	}
}

func TestScanTimeoutReleasePromotesNext(t *testing.T) {
	w := New(1, time.Minute)
	mustJoin(t, w, "app-1", "app-2")
	p := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	res := w.Scan(base.Add(2 * time.Minute))
	if len(res.Expired) != 1 || res.Expired[0].ID != p.ID {
		t.Fatalf("expired = %+v, want [%s]", res.Expired, p.ID)
	}
	if len(res.Promoted) != 1 || res.Promoted[0].ApplicationID != "app-2" {
		t.Fatalf("promoted = %+v, want app-2", res.Promoted)
	}
	view := w.View()
	if view.Slots[0].State != SlotReserved || view.Slots[0].PromotionID != res.Promoted[0].ID {
		t.Fatalf("slot = %+v, want reserved by new promotion", view.Slots[0])
	}
	// Ledger must show release with reason timeout then re-reserve.
	var actions []string
	for _, e := range view.Events {
		actions = append(actions, e.Action+":"+e.Reason)
	}
	want := []string{"reserved:promoted", "released:timeout", "reserved:promoted"}
	if len(actions) != len(want) {
		t.Fatalf("events = %v, want %v", actions, want)
	}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("events = %v, want %v", actions, want)
		}
	}
}

func TestScanDoesNotRepromoteDeclined(t *testing.T) {
	w := New(1, time.Minute)
	mustJoin(t, w, "app-1", "app-2")
	p := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	if _, err := w.Decline(p.ID, base); err != nil {
		t.Fatalf("Decline: %v", err)
	}
	res := w.Scan(base)
	if len(res.Promoted) != 1 || res.Promoted[0].ApplicationID != "app-2" {
		t.Fatalf("promoted = %+v, want app-2 only", res.Promoted)
	}
	// Later scans must not bring the declined application back.
	res = w.Scan(base.Add(2 * time.Minute))
	for _, promo := range res.Promoted {
		if promo.ApplicationID == "app-1" {
			t.Fatalf("declined app-1 re-promoted: %+v", promo)
		}
	}
}

func TestRepeatedScanIsNoop(t *testing.T) {
	w := New(1, time.Minute)
	mustJoin(t, w, "app-1")
	mustPromote(t, w, "promo-1", base.Add(time.Minute))

	first := w.Scan(base.Add(2 * time.Minute))
	if len(first.Expired) != 1 {
		t.Fatalf("first scan expired = %+v, want 1", first.Expired)
	}
	eventsBefore := len(w.View().Events)
	second := w.Scan(base.Add(3 * time.Minute))
	if len(second.Expired) != 0 || len(second.Promoted) != 0 {
		t.Fatalf("second scan = %+v, want empty", second)
	}
	if got := len(w.View().Events); got != eventsBefore {
		t.Fatalf("events grew on repeated scan: %d -> %d", eventsBefore, got)
	}
}

func TestPromoteIdempotentReplayAndConflict(t *testing.T) {
	w := New(2, time.Minute)
	mustJoin(t, w, "app-1", "app-2")
	req := PromoteRequest{ID: "promo-1", NotificationID: "notif-1", SlotID: "slot-2", Deadline: base.Add(time.Minute)}

	p, err := w.Promote(req, base)
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	replay, err := w.Promote(req, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !reflect.DeepEqual(replay, p) {
		t.Fatalf("replay = %+v, want %+v", replay, p)
	}

	changedDeadline := req
	changedDeadline.Deadline = base.Add(2 * time.Minute)
	if _, err := w.Promote(changedDeadline, base); err != ErrConflict {
		t.Fatalf("changed deadline err = %v, want ErrConflict", err)
	}
	changedSlot := req
	changedSlot.SlotID = "slot-1"
	if _, err := w.Promote(changedSlot, base); err != ErrConflict {
		t.Fatalf("changed slot err = %v, want ErrConflict", err)
	}
	// The conflicting replays must not consume queue entries or slots.
	if got := len(w.View().Queue); got != 1 {
		t.Fatalf("queue len = %d, want 1", got)
	}
}

func TestTerminalStateReplayAndClosed(t *testing.T) {
	w := New(2, time.Minute)
	mustJoin(t, w, "app-1", "app-2")
	p1 := mustPromote(t, w, "promo-1", base.Add(time.Minute))

	if _, err := w.Confirm(p1.ID, base); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Confirm(p1.ID, base); err != nil {
		t.Fatalf("confirm replay: %v", err)
	}
	if _, err := w.Decline(p1.ID, base); err != ErrClosed {
		t.Fatalf("decline after confirm err = %v, want ErrClosed", err)
	}

	p2 := mustPromote(t, w, "promo-2", base.Add(time.Minute))
	if _, err := w.Decline(p2.ID, base); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Decline(p2.ID, base); err != nil {
		t.Fatalf("decline replay: %v", err)
	}
	if _, err := w.Confirm(p2.ID, base); err != ErrClosed {
		t.Fatalf("confirm after decline err = %v, want ErrClosed", err)
	}
}

func TestConcurrentResolveSingleTerminalState(t *testing.T) {
	for i := 0; i < 50; i++ {
		w := New(1, time.Minute)
		mustJoin(t, w, "app-1")
		p := mustPromote(t, w, "promo-1", base.Add(time.Minute))

		var wg sync.WaitGroup
		results := make([]error, 3)
		ops := []func() error{
			func() error { _, err := w.Confirm(p.ID, base); return err },
			func() error { _, err := w.Decline(p.ID, base); return err },
			func() error { w.Scan(base.Add(2 * time.Minute)); return nil },
		}
		for j, op := range ops {
			wg.Add(1)
			go func(j int, op func() error) {
				defer wg.Done()
				results[j] = op()
			}(j, op)
		}
		wg.Wait()

		view := w.View()
		state := view.Promotions[0].State
		if state == PromotionPending {
			t.Fatalf("iter %d: promotion still pending after concurrent resolve", i)
		}
		// The slot must have been occupied or released exactly once in
		// total across the ledger.
		occupied, released := 0, 0
		for _, e := range view.Events {
			switch e.Action {
			case ActionOccupied:
				occupied++
			case ActionReleased:
				released++
			}
		}
		if occupied+released != 1 {
			t.Fatalf("iter %d: occupied=%d released=%d, want exactly one terminal slot event", i, occupied, released)
		}
		switch state {
		case PromotionConfirmed:
			if occupied != 1 || view.Slots[0].State != SlotOccupied {
				t.Fatalf("iter %d: confirmed but slot events inconsistent", i)
			}
		case PromotionDeclined, PromotionExpired:
			if released != 1 || view.Slots[0].State != SlotFree {
				t.Fatalf("iter %d: %s but slot not freed exactly once", i, state)
			}
		}
	}
}

func TestViewSnapshot(t *testing.T) {
	w := New(2, time.Minute)
	mustJoin(t, w, "app-1", "app-2", "app-3")
	p1 := mustPromote(t, w, "promo-1", base.Add(time.Minute))
	if _, err := w.Confirm(p1.ID, base); err != nil {
		t.Fatal(err)
	}

	view := w.View()
	if len(view.Queue) != 2 || view.Queue[0].ID != "app-2" || view.Queue[1].ID != "app-3" {
		t.Fatalf("queue = %+v, want [app-2 app-3]", view.Queue)
	}
	if len(view.Promotions) != 1 || view.Promotions[0].State != PromotionConfirmed {
		t.Fatalf("promotions = %+v", view.Promotions)
	}
	if view.Slots[0].State != SlotOccupied || view.Slots[1].State != SlotFree {
		t.Fatalf("slots = %+v", view.Slots)
	}
	if len(view.Events) != 2 || view.Events[1].Reason != ReasonConfirmed {
		t.Fatalf("events = %+v", view.Events)
	}
}
