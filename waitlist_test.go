package goreservationwaitlist

import (
	"errors"
	"sync"
	"testing"
	"time"
)

var base = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

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

func slotEvents(v View, slotID, action string) []SlotEvent {
	var out []SlotEvent
	for _, e := range v.Events {
		if e.SlotID == slotID && e.Action == action {
			out = append(out, e)
		}
	}
	return out
}

// 确认边界：截止时间含边界，恰好等于截止时间可确认，超过则过期释放。
func TestConfirmDeadlineBoundary(t *testing.T) {
	w := New(1, time.Hour)
	mustJoin(t, w, "A")
	p := mustPromote(t, w, "P1", base.Add(time.Hour))

	confirmed, err := w.Confirm(p.ID, p.Deadline)
	if err != nil {
		t.Fatalf("confirm at exact deadline should succeed: %v", err)
	}
	if confirmed.State != PromotionConfirmed {
		t.Fatalf("state = %s, want confirmed", confirmed.State)
	}

	w2 := New(1, time.Hour)
	mustJoin(t, w2, "A")
	p2 := mustPromote(t, w2, "P1", base.Add(time.Hour))
	if _, err := w2.Confirm(p2.ID, p2.Deadline.Add(time.Nanosecond)); !errors.Is(err, ErrExpired) {
		t.Fatalf("confirm after deadline: err = %v, want ErrExpired", err)
	}
	v := w2.View()
	if got := slotEvents(v, p2.SlotID, ActionReleased); len(got) != 1 || got[0].Reason != ReasonTimeout {
		t.Fatalf("late confirm must release the slot once with reason timeout: %+v", got)
	}
}

// 候补条件变化后旧晋级不能继续确认。
func TestConfirmStaleVersionRejected(t *testing.T) {
	w := New(1, time.Hour)
	mustJoin(t, w, "A")
	p := mustPromote(t, w, "P1", base.Add(time.Hour))

	mustJoin(t, w, "B") // 候补条件变化，版本前进

	if _, err := w.Confirm(p.ID, base.Add(time.Minute)); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("err = %v, want ErrStaleVersion", err)
	}
	if got := w.View().Slots[0].State; got != SlotReserved {
		t.Fatalf("slot state = %s, want reserved (未释放也未占用)", got)
	}
}

// 超时释放后按当前队列快照选择下一位。
func TestScanTimeoutReleasePromotesNext(t *testing.T) {
	w := New(1, time.Hour)
	mustJoin(t, w, "A", "B", "C")
	p := mustPromote(t, w, "P1", base.Add(time.Hour))

	res := w.Scan(base.Add(2 * time.Hour))
	if len(res.Expired) != 1 || res.Expired[0].ID != "P1" {
		t.Fatalf("expired = %+v, want [P1]", res.Expired)
	}
	if len(res.Promoted) != 1 || res.Promoted[0].ApplicationID != "B" {
		t.Fatalf("promoted = %+v, want B", res.Promoted)
	}
	np := res.Promoted[0]
	if np.SlotID != p.SlotID {
		t.Fatalf("slot %s should be reused, got %s", p.SlotID, np.SlotID)
	}
	if !np.Deadline.Equal(base.Add(3 * time.Hour)) {
		t.Fatalf("new deadline = %v, want scan time + ttl", np.Deadline)
	}
	v := w.View()
	if got := slotEvents(v, p.SlotID, ActionReleased); len(got) != 1 || got[0].Reason != ReasonTimeout {
		t.Fatalf("released events = %+v, want one timeout release", got)
	}
	if len(v.Queue) != 1 || v.Queue[0].ID != "C" {
		t.Fatalf("queue = %+v, want [C]", v.Queue)
	}
}

// 已明确放弃的申请不能被同一次扫描再次晋级。
func TestScanSkipsDeclinedApplications(t *testing.T) {
	w := New(1, time.Hour)
	mustJoin(t, w, "A", "B", "C")
	pa := mustPromote(t, w, "PA", base.Add(time.Hour))
	if _, err := w.Decline(pa.ID, base.Add(time.Minute)); err != nil {
		t.Fatalf("Decline: %v", err)
	}
	pb := mustPromote(t, w, "PB", base.Add(2*time.Hour))
	if _, err := w.Decline(pb.ID, base.Add(2*time.Minute)); err != nil {
		t.Fatalf("Decline: %v", err)
	}

	res := w.Scan(base.Add(3 * time.Minute))
	if len(res.Expired) != 0 {
		t.Fatalf("expired = %+v, want none", res.Expired)
	}
	if len(res.Promoted) != 1 || res.Promoted[0].ApplicationID != "C" {
		t.Fatalf("promoted = %+v, want only C", res.Promoted)
	}
	for _, p := range res.Promoted {
		if p.ApplicationID == "A" || p.ApplicationID == "B" {
			t.Fatalf("declined application %s re-promoted", p.ApplicationID)
		}
	}
}

// 重复扫描是空操作：名额不会被重复释放，也不会重复晋级。
func TestScanIdempotent(t *testing.T) {
	w := New(1, time.Hour)
	mustJoin(t, w, "A", "B")
	p := mustPromote(t, w, "P1", base.Add(time.Hour))

	first := w.Scan(base.Add(2 * time.Hour))
	if len(first.Expired) != 1 || len(first.Promoted) != 1 {
		t.Fatalf("first scan = %+v", first)
	}
	eventsAfterFirst := len(w.View().Events)

	second := w.Scan(base.Add(2 * time.Hour))
	if len(second.Expired) != 0 || len(second.Promoted) != 0 {
		t.Fatalf("second scan = %+v, want empty", second)
	}
	if got := len(w.View().Events); got != eventsAfterFirst {
		t.Fatalf("events grew from %d to %d after duplicate scan", eventsAfterFirst, got)
	}
	if got := slotEvents(w.View(), p.SlotID, ActionReleased); len(got) != 1 {
		t.Fatalf("slot released %d times, want exactly 1", len(got))
	}
}

// 确认、放弃、超时扫描并发争抢时只能形成一个终态，名额最多释放或占用一次。
func TestConcurrentResolutionSingleTerminalState(t *testing.T) {
	for i := 0; i < 100; i++ {
		w := New(1, time.Hour)
		mustJoin(t, w, "A")
		p := mustPromote(t, w, "P1", base.Add(time.Hour))

		var wg sync.WaitGroup
		var confirmErr, declineErr error
		var scanRes ScanResult
		wg.Add(3)
		go func() { defer wg.Done(); _, confirmErr = w.Confirm(p.ID, base.Add(30*time.Minute)) }()
		go func() { defer wg.Done(); _, declineErr = w.Decline(p.ID, base.Add(30*time.Minute)) }()
		go func() { defer wg.Done(); scanRes = w.Scan(base.Add(2 * time.Hour)) }()
		wg.Wait()

		won := 0
		if confirmErr == nil {
			won++
		}
		if declineErr == nil {
			won++
		}
		if len(scanRes.Expired) == 1 {
			won++
		}
		if won != 1 {
			t.Fatalf("iter %d: %d competing resolutions succeeded, want exactly 1", i, won)
		}

		v := w.View()
		terminal := len(slotEvents(v, p.SlotID, ActionOccupied)) + len(slotEvents(v, p.SlotID, ActionReleased))
		if terminal != 1 {
			t.Fatalf("iter %d: slot has %d terminal events, want exactly 1", i, terminal)
		}
		got := v.Promotions[0]
		switch {
		case confirmErr == nil && got.State != PromotionConfirmed,
			declineErr == nil && got.State != PromotionDeclined,
			len(scanRes.Expired) == 1 && got.State != PromotionExpired:
			t.Fatalf("iter %d: final state %s inconsistent with winner", i, got.State)
		}
	}
}

// 相同晋级号相同内容重放返回原结果；名额或截止时间变化返回冲突。
func TestPromoteReplayAndConflict(t *testing.T) {
	w := New(2, time.Hour)
	mustJoin(t, w, "A", "B")
	req := PromoteRequest{ID: "P1", NotificationID: "N1", SlotID: "slot-1", Deadline: base.Add(time.Hour)}
	p, err := w.Promote(req, base)
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}

	again, err := w.Promote(req, base.Add(30*time.Minute))
	if err != nil {
		t.Fatalf("identical replay should return original: %v", err)
	}
	if again != p {
		t.Fatalf("replay = %+v, want %+v", again, p)
	}

	changedSlot := req
	changedSlot.SlotID = "slot-2"
	if _, err := w.Promote(changedSlot, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("slot change: err = %v, want ErrConflict", err)
	}
	changedDeadline := req
	changedDeadline.Deadline = base.Add(2 * time.Hour)
	if _, err := w.Promote(changedDeadline, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("deadline change: err = %v, want ErrConflict", err)
	}

	if len(w.View().Promotions) != 1 {
		t.Fatalf("replay created extra promotions")
	}

	if _, err := w.Confirm(p.ID, base.Add(time.Minute)); err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	replayed, err := w.Confirm(p.ID, base.Add(time.Minute))
	if err != nil || replayed.State != PromotionConfirmed {
		t.Fatalf("confirm replay = %+v, %v", replayed, err)
	}
	if got := slotEvents(w.View(), p.SlotID, ActionOccupied); len(got) != 1 {
		t.Fatalf("slot occupied %d times, want exactly 1", len(got))
	}
}

// 查询能看到候补顺序、每次晋级、确认结果和名额变动原因。
func TestViewSnapshot(t *testing.T) {
	w := New(1, time.Hour)
	mustJoin(t, w, "A", "B", "C")
	p := mustPromote(t, w, "P1", base.Add(time.Hour))
	if _, err := w.Decline(p.ID, base.Add(time.Minute)); err != nil {
		t.Fatalf("Decline: %v", err)
	}
	res := w.Scan(base.Add(2 * time.Minute))
	if len(res.Promoted) != 1 {
		t.Fatalf("scan promoted = %+v", res.Promoted)
	}
	if _, err := w.Confirm(res.Promoted[0].ID, base.Add(3*time.Minute)); err != nil {
		t.Fatalf("Confirm: %v", err)
	}

	v := w.View()
	if len(v.Queue) != 1 || v.Queue[0].ID != "C" {
		t.Fatalf("queue = %+v, want [C]", v.Queue)
	}
	if len(v.Promotions) != 2 {
		t.Fatalf("promotions = %+v, want 2 records", v.Promotions)
	}
	if v.Promotions[0].State != PromotionDeclined || v.Promotions[1].State != PromotionConfirmed {
		t.Fatalf("promotion states = %s, %s", v.Promotions[0].State, v.Promotions[1].State)
	}
	wantReasons := []struct{ action, reason string }{
		{ActionReserved, ReasonPromoted},
		{ActionReleased, ReasonDeclined},
		{ActionReserved, ReasonPromoted},
		{ActionOccupied, ReasonConfirmed},
	}
	if len(v.Events) != len(wantReasons) {
		t.Fatalf("events = %+v", v.Events)
	}
	for i, want := range wantReasons {
		if v.Events[i].Action != want.action || v.Events[i].Reason != want.reason {
			t.Fatalf("event %d = %s/%s, want %s/%s",
				i, v.Events[i].Action, v.Events[i].Reason, want.action, want.reason)
		}
	}
	if v.Slots[0].State != SlotOccupied {
		t.Fatalf("slot state = %s, want occupied", v.Slots[0].State)
	}
}
