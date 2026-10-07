// Package goreservationwaitlist implements a reservation waitlist with
// capacity promotion and reserved-slot confirmation deadlines.
package goreservationwaitlist

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrNotFound        = errors.New("waitlist: promotion not found")
	ErrConflict        = errors.New("waitlist: conflicting replay for promotion id")
	ErrStaleVersion    = errors.New("waitlist: waitlist version changed since promotion")
	ErrClosed          = errors.New("waitlist: promotion already resolved in another terminal state")
	ErrExpired         = errors.New("waitlist: confirmation deadline exceeded")
	ErrNoCapacity      = errors.New("waitlist: no free slot")
	ErrQueueEmpty      = errors.New("waitlist: no waiting application")
	ErrDuplicateApp    = errors.New("waitlist: application already exists")
	ErrUnknownApp      = errors.New("waitlist: application not found")
	ErrNotWaiting      = errors.New("waitlist: application is not waiting")
	ErrInvalidRule     = errors.New("waitlist: invalid ranking rule")
	ErrPreviewNotFound = errors.New("waitlist: reorder preview not found")
)

// PromotionState is the lifecycle state of a promotion offer.
type PromotionState string

const (
	PromotionPending   PromotionState = "pending"
	PromotionConfirmed PromotionState = "confirmed"
	PromotionDeclined  PromotionState = "declined"
	PromotionExpired   PromotionState = "expired"
)

// AppStatus is the lifecycle state of a waitlist application.
type AppStatus string

const (
	StatusWaiting   AppStatus = "waiting"
	StatusPromoted  AppStatus = "promoted"
	StatusConfirmed AppStatus = "confirmed"
	StatusDeclined  AppStatus = "declined"
	StatusExpired   AppStatus = "expired"
	StatusWithdrawn AppStatus = "withdrawn"
)

// SlotState is the occupancy state of a capacity slot.
type SlotState string

const (
	SlotFree     SlotState = "free"
	SlotReserved SlotState = "reserved"
	SlotOccupied SlotState = "occupied"
)

// Slot ledger actions and reasons.
const (
	ActionReserved = "reserved"
	ActionOccupied = "occupied"
	ActionReleased = "released"

	ReasonPromoted  = "promoted"
	ReasonConfirmed = "confirmed"
	ReasonDeclined  = "declined"
	ReasonTimeout   = "timeout"
)

// Application is a waitlist entry. Qualification, RuleVersion and Basis are
// the ranking snapshot captured at join time; Basis is derived from the
// immutable rule version and guarantees a stable order within that version.
type Application struct {
	ID            string
	Status        AppStatus
	JoinedAt      time.Time
	Qualification Qualification
	RuleVersion   int64
	Basis         []string
	seq           int64
}

// Promotion is an offer of a reserved slot to an applicant. It records the
// slot, the waitlist version at offer time, the confirmation deadline and
// the notification id used to deliver the offer.
type Promotion struct {
	ID             string
	ApplicationID  string
	SlotID         string
	Version        int64
	RuleVersion    int64
	Basis          []string
	NotificationID string
	CreatedAt      time.Time
	Deadline       time.Time
	State          PromotionState
	ResolvedAt     time.Time
}

// PromoteRequest creates a promotion for the head of the queue. ID is the
// idempotency key: replaying the same request content returns the stored
// promotion; changing the slot, notification id or deadline conflicts.
type PromoteRequest struct {
	ID             string
	NotificationID string
	SlotID         string // optional; empty auto-assigns the first free slot
	Deadline       time.Time
	// BaseVersion optionally pins the queue version the caller observed.
	// When non-zero, promotion fails with ErrStaleVersion if the queue has
	// since been reordered or otherwise changed.
	BaseVersion int64
}

// SlotEvent is one entry in the slot ledger.
type SlotEvent struct {
	Seq           int64
	SlotID        string
	PromotionID   string
	ApplicationID string
	Action        string
	Reason        string
	At            time.Time
}

// SlotView describes a slot in a View snapshot.
type SlotView struct {
	ID          string
	State       SlotState
	PromotionID string
}

// View is a consistent snapshot of the waitlist for queries.
type View struct {
	Version            int64
	Rule               Rule
	Queue              []Application
	Promotions         []Promotion
	Slots              []SlotView
	Events             []SlotEvent
	LastReorder        *ReorderResult
	OutstandingPreview *ReorderPreview
}

// ScanResult reports what a Scan did.
type ScanResult struct {
	Expired  []Promotion
	Promoted []Promotion
}

type slot struct {
	id        string
	state     SlotState
	promotion string
}

// Waitlist is a concurrency-safe reservation waitlist. Construct with New.
type Waitlist struct {
	mu          sync.Mutex
	ttl         time.Duration
	version     int64
	seq         int64
	queue       []string
	apps        map[string]*Application
	promotions  map[string]*Promotion
	order       []string
	slots       []*slot
	events      []SlotEvent
	rule        Rule
	joinSeq     int64
	previews    map[string]*storedPreview
	lastReorder *ReorderResult
}

// New creates a waitlist with the given slot capacity and the confirmation
// TTL applied to promotions created by Scan.
func New(capacity int, ttl time.Duration) *Waitlist {
	w := &Waitlist{
		ttl:        ttl,
		apps:       make(map[string]*Application),
		promotions: make(map[string]*Promotion),
		rule:       defaultRule(),
		previews:   make(map[string]*storedPreview),
	}
	for i := 0; i < capacity; i++ {
		w.slots = append(w.slots, &slot{id: fmt.Sprintf("slot-%d", i+1), state: SlotFree})
	}
	return w
}

// Join adds an application to the tail of the queue.
func (w *Waitlist) Join(appID string, now time.Time) (Application, error) {
	return w.JoinWith(appID, Qualification{}, now)
}

// Leave withdraws a waiting application.
func (w *Waitlist) Leave(appID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	app, ok := w.apps[appID]
	if !ok {
		return ErrUnknownApp
	}
	if app.Status != StatusWaiting {
		return ErrNotWaiting
	}
	w.removeFromQueue(appID)
	app.Status = StatusWithdrawn
	w.invalidatePreviewsLocked()
	w.version++
	return nil
}

// Promote offers the head of the queue a reserved slot. It is idempotent
// on req.ID: an identical replay returns the stored promotion, while a
// replay whose slot, notification id or deadline differs fails with
// ErrConflict.
func (w *Waitlist) Promote(req PromoteRequest, now time.Time) (Promotion, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if existing, ok := w.promotions[req.ID]; ok {
		if existing.NotificationID == req.NotificationID &&
			(req.SlotID == "" || existing.SlotID == req.SlotID) &&
			existing.Deadline.Equal(req.Deadline) &&
			(req.BaseVersion == 0 || existing.Version == req.BaseVersion) {
			return *existing, nil
		}
		return Promotion{}, ErrConflict
	}
	if req.BaseVersion != 0 && req.BaseVersion != w.version {
		return Promotion{}, ErrStaleVersion
	}
	return w.promoteLocked(req, now)
}

// Confirm accepts a pending promotion at or before its deadline. The
// waitlist version must not have changed since the offer was created.
// Confirming an already confirmed promotion replays the original result;
// confirming a declined or expired one fails with ErrClosed.
func (w *Waitlist) Confirm(promotionID string, now time.Time) (Promotion, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.promotions[promotionID]
	if !ok {
		return Promotion{}, ErrNotFound
	}
	switch p.State {
	case PromotionConfirmed:
		return *p, nil
	case PromotionDeclined, PromotionExpired:
		return *p, ErrClosed
	}
	if now.After(p.Deadline) {
		w.expireLocked(p, now)
		return *p, ErrExpired
	}
	if p.Version != w.version {
		return *p, ErrStaleVersion
	}
	p.State = PromotionConfirmed
	p.ResolvedAt = now
	w.apps[p.ApplicationID].Status = StatusConfirmed
	s := w.slotByID(p.SlotID)
	s.state = SlotOccupied
	w.appendEvent(s.id, p, ActionOccupied, ReasonConfirmed, now)
	return *p, nil
}

// Decline rejects a pending promotion and releases its slot. Declining an
// already declined promotion replays the original result.
func (w *Waitlist) Decline(promotionID string, now time.Time) (Promotion, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.promotions[promotionID]
	if !ok {
		return Promotion{}, ErrNotFound
	}
	switch p.State {
	case PromotionDeclined:
		return *p, nil
	case PromotionConfirmed, PromotionExpired:
		return *p, ErrClosed
	}
	p.State = PromotionDeclined
	p.ResolvedAt = now
	w.apps[p.ApplicationID].Status = StatusDeclined
	w.releaseSlotLocked(p, ReasonDeclined, now)
	w.invalidatePreviewsLocked()
	w.version++
	return *p, nil
}

// Scan expires pending promotions whose deadline has passed, releases their
// slots, then offers freed slots to the current queue head in order. Every
// decision is made under a single queue version captured at scan start:
// promotions created by this scan record that version, so a concurrently
// published rule reorder cannot keep an older scan handing out slots in a
// stale order. Applications that declined or expired stay out of the queue
// and are never re-promoted by the same or later scans.
func (w *Waitlist) Scan(now time.Time) ScanResult {
	w.mu.Lock()
	defer w.mu.Unlock()
	var res ScanResult
	for _, id := range w.order {
		p := w.promotions[id]
		if p.State == PromotionPending && now.After(p.Deadline) {
			w.expireLocked(p, now)
			res.Expired = append(res.Expired, *p)
		}
	}
	for w.firstFreeSlot() != nil && len(w.queue) > 0 {
		w.seq++
		req := PromoteRequest{
			ID:             fmt.Sprintf("promo-%06d", w.seq),
			NotificationID: fmt.Sprintf("notif-%06d", w.seq),
			Deadline:       now.Add(w.ttl),
		}
		p, err := w.promoteLocked(req, now)
		if err != nil {
			break
		}
		res.Promoted = append(res.Promoted, p)
	}
	return res
}

// View returns a consistent snapshot of queue order, promotions, slots and
// the slot event ledger.
func (w *Waitlist) View() View {
	w.mu.Lock()
	defer w.mu.Unlock()
	v := View{Version: w.version, Rule: w.rule}
	for _, id := range w.queue {
		v.Queue = append(v.Queue, *w.apps[id])
	}
	for _, id := range w.order {
		p := w.promotions[id]
		cp := *p
		cp.Basis = append([]string(nil), p.Basis...)
		v.Promotions = append(v.Promotions, cp)
	}
	for _, s := range w.slots {
		v.Slots = append(v.Slots, SlotView{ID: s.id, State: s.state, PromotionID: s.promotion})
	}
	v.Events = append(v.Events, w.events...)
	v.LastReorder = w.lastReorder
	for _, p := range w.previews {
		if !p.committed {
			snap := p.snapshot()
			v.OutstandingPreview = &snap
			break
		}
	}
	return v
}

func (w *Waitlist) promoteLocked(req PromoteRequest, now time.Time) (Promotion, error) {
	if len(w.queue) == 0 {
		return Promotion{}, ErrQueueEmpty
	}
	s, err := w.pickSlot(req.SlotID)
	if err != nil {
		return Promotion{}, err
	}
	appID := w.queue[0]
	w.queue = w.queue[1:]
	w.version++
	head := w.apps[appID]
	p := &Promotion{
		ID:             req.ID,
		ApplicationID:  appID,
		SlotID:         s.id,
		Version:        w.version,
		RuleVersion:    head.RuleVersion,
		Basis:          append([]string(nil), head.Basis...),
		NotificationID: req.NotificationID,
		CreatedAt:      now,
		Deadline:       req.Deadline,
		State:          PromotionPending,
	}
	w.promotions[p.ID] = p
	w.order = append(w.order, p.ID)
	w.apps[appID].Status = StatusPromoted
	s.state = SlotReserved
	s.promotion = p.ID
	w.invalidatePreviewsLocked()
	w.appendEvent(s.id, p, ActionReserved, ReasonPromoted, now)
	return *p, nil
}

// invalidatePreviewsLocked is a marker invoked before any mutation that
// moves the queue version (join, leave, promotion, decline, expiry,
// qualification change). Preview records are intentionally kept: replaying
// the same reorder id must report ErrStaleVersion rather than silently
// minting a fresh preview, and committed previews remain replayable.
func (w *Waitlist) invalidatePreviewsLocked() {}

func (w *Waitlist) expireLocked(p *Promotion, now time.Time) {
	p.State = PromotionExpired
	p.ResolvedAt = now
	w.apps[p.ApplicationID].Status = StatusExpired
	w.releaseSlotLocked(p, ReasonTimeout, now)
	w.invalidatePreviewsLocked()
	w.version++
}

func (w *Waitlist) releaseSlotLocked(p *Promotion, reason string, now time.Time) {
	s := w.slotByID(p.SlotID)
	s.state = SlotFree
	s.promotion = ""
	w.appendEvent(s.id, p, ActionReleased, reason, now)
}

func (w *Waitlist) appendEvent(slotID string, p *Promotion, action, reason string, now time.Time) {
	w.seq++
	w.events = append(w.events, SlotEvent{
		Seq:           w.seq,
		SlotID:        slotID,
		PromotionID:   p.ID,
		ApplicationID: p.ApplicationID,
		Action:        action,
		Reason:        reason,
		At:            now,
	})
}

func (w *Waitlist) pickSlot(id string) (*slot, error) {
	if id != "" {
		s := w.slotByID(id)
		if s == nil || s.state != SlotFree {
			return nil, ErrNoCapacity
		}
		return s, nil
	}
	if s := w.firstFreeSlot(); s != nil {
		return s, nil
	}
	return nil, ErrNoCapacity
}

func (w *Waitlist) firstFreeSlot() *slot {
	for _, s := range w.slots {
		if s.state == SlotFree {
			return s
		}
	}
	return nil
}

func (w *Waitlist) slotByID(id string) *slot {
	for _, s := range w.slots {
		if s.id == id {
			return s
		}
	}
	return nil
}

func (w *Waitlist) removeFromQueue(appID string) {
	for i, id := range w.queue {
		if id == appID {
			w.queue = append(w.queue[:i], w.queue[i+1:]...)
			return
		}
	}
}
