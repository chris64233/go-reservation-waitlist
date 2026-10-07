package goreservationwaitlist

import (
	"fmt"
	"sort"
	"time"
)

// CriterionID identifies one ordered ranking criterion of a Rule.
type CriterionID string

const (
	// CriterionWaitTime ranks earlier JoinedAt first.
	CriterionWaitTime CriterionID = "wait-time"
	// CriterionTier ranks higher qualification tier first.
	CriterionTier CriterionID = "tier"
	// CriterionScore ranks higher qualification score first.
	CriterionScore CriterionID = "score"
)

// Qualification is the eligibility information captured for an application.
// It is snapshotted at join time and never mutated implicitly.
type Qualification struct {
	Tier  int
	Score int
}

// Rule is an immutable, ordered set of ranking criteria. A Rule value must
// never be mutated after it has been published or attached to a preview.
type Rule struct {
	Version  int64
	Criteria []CriterionID
}

func (r Rule) equal(other Rule) bool {
	if r.Version != other.Version || len(r.Criteria) != len(other.Criteria) {
		return false
	}
	for i := range r.Criteria {
		if r.Criteria[i] != other.Criteria[i] {
			return false
		}
	}
	return true
}

// PositionChange describes how one waiting application moves when a new rule
// is applied. From and To are zero-based positions in the waiting queue.
type PositionChange struct {
	ApplicationID string
	From          int
	To            int
	Reason        string
}

// ReorderRequest asks for a reorder preview. ID is the idempotency key:
// replaying the same ID with the same rule against the same queue version
// returns the original preview, while changing either one conflicts.
type ReorderRequest struct {
	ID   string
	Rule Rule
}

// ReorderPreview is the not-yet-published result of applying a rule. It is
// bound to BaselineVersion and becomes unusable once the waiting queue
// baseline changes.
type ReorderPreview struct {
	ID              string
	Rule            Rule
	FromRule        Rule
	BaselineVersion int64
	Order           []string
	Changes         []PositionChange
}

// ReorderResult is the published outcome of committing a preview.
type ReorderResult struct {
	ReorderPreview
	NewVersion int64
}

func defaultRule() Rule {
	return Rule{Version: 1, Criteria: []CriterionID{CriterionWaitTime}}
}

// CurrentRule returns the rule currently applied to the waiting queue.
func (w *Waitlist) CurrentRule() Rule {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rule
}

// JoinWith joins the queue with explicit qualification information. The
// qualification and the ranking basis derived under the current rule are
// snapshotted on the application.
func (w *Waitlist) JoinWith(appID string, q Qualification, now time.Time) (Application, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.apps[appID]; ok {
		return Application{}, ErrDuplicateApp
	}
	w.joinSeq++
	app := &Application{
		ID:            appID,
		Status:        StatusWaiting,
		JoinedAt:      now,
		Qualification: q,
		RuleVersion:   w.rule.Version,
		Basis:         basisOf(q, now, w.joinSeq, appID, w.rule),
		seq:           w.joinSeq,
	}
	w.apps[appID] = app
	w.queue = append(w.queue, appID)
	w.sortQueueLocked()
	w.invalidatePreviewsLocked()
	w.version++
	return *app, nil
}

// UpdateQualification records a qualification change for a waiting
// application and re-ranks it under the current rule. Any outstanding
// reorder preview is invalidated because the queue baseline changes.
func (w *Waitlist) UpdateQualification(appID string, q Qualification) (Application, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	app, ok := w.apps[appID]
	if !ok {
		return Application{}, ErrUnknownApp
	}
	if app.Status != StatusWaiting {
		return Application{}, ErrNotWaiting
	}
	app.Qualification = q
	app.RuleVersion = w.rule.Version
	app.Basis = basisOf(q, app.JoinedAt, app.seq, appID, w.rule)
	w.sortQueueLocked()
	w.invalidatePreviewsLocked()
	w.version++
	return *app, nil
}

// PreviewReorder computes the waiting-queue order under req.Rule without
// publishing it. Promoted applications (including pending offers inside
// their retention window) are never part of the preview and keep their
// existing promotion result.
func (w *Waitlist) PreviewReorder(req ReorderRequest) (ReorderPreview, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if existing, ok := w.previews[req.ID]; ok {
		if existing.committed {
			return existing.snapshot(), nil
		}
		if !existing.rule.equal(req.Rule) {
			return ReorderPreview{}, ErrConflict
		}
		if existing.baselineVersion != w.version {
			return ReorderPreview{}, ErrStaleVersion
		}
		return existing.snapshot(), nil
	}
	if len(req.Rule.Criteria) == 0 {
		return ReorderPreview{}, ErrInvalidRule
	}
	for _, c := range req.Rule.Criteria {
		if !knownCriterion(c) {
			return ReorderPreview{}, ErrInvalidRule
		}
	}

	oldOrder := append([]string(nil), w.queue...)
	newOrder := append([]string(nil), w.queue...)
	sort.SliceStable(newOrder, func(i, j int) bool {
		return w.lessUnderRule(newOrder[i], newOrder[j], req.Rule)
	})

	preview := &storedPreview{
		id:              req.ID,
		rule:            req.Rule,
		fromRule:        w.rule,
		baselineVersion: w.version,
		order:           newOrder,
		changes:         w.positionChangesLocked(oldOrder, newOrder, req.Rule),
	}
	w.previews[req.ID] = preview
	return preview.snapshot(), nil
}

// CommitReorder publishes a previewed rule atomically. It fails with
// ErrStaleVersion if the queue baseline moved after the preview (new join,
// qualification change, promotion, withdrawal or slot release); in that
// case nothing is applied and a fresh preview must be requested.
func (w *Waitlist) CommitReorder(previewID string) (ReorderResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	preview, ok := w.previews[previewID]
	if !ok {
		return ReorderResult{}, ErrPreviewNotFound
	}
	if preview.committed {
		return preview.result(), nil
	}
	if preview.baselineVersion != w.version {
		return ReorderResult{}, ErrStaleVersion
	}

	sort.SliceStable(w.queue, func(i, j int) bool {
		return w.lessUnderRule(w.queue[i], w.queue[j], preview.rule)
	})
	for i, id := range w.queue {
		if id != preview.order[i] {
			// The preview order must be applied as one whole unit.
			return ReorderResult{}, ErrStaleVersion
		}
	}
	w.rule = preview.rule
	for _, id := range w.queue {
		app := w.apps[id]
		app.RuleVersion = w.rule.Version
		app.Basis = basisOf(app.Qualification, app.JoinedAt, app.seq, app.ID, w.rule)
	}
	w.version++
	preview.committed = true
	preview.newVersion = w.version
	// Offers inside their retention window keep their result: re-anchor
	// their version so confirmation stays valid after the publish, and
	// the reserved slot is never silently reassigned by the new queue.
	for _, pid := range w.order {
		if p := w.promotions[pid]; p.State == PromotionPending {
			p.Version = w.version
		}
	}
	w.lastReorder = &ReorderResult{
		ReorderPreview: preview.snapshot(),
		NewVersion:     w.version,
	}
	return preview.result(), nil
}

type storedPreview struct {
	id              string
	rule            Rule
	fromRule        Rule
	baselineVersion int64
	order           []string
	changes         []PositionChange
	committed       bool
	newVersion      int64
}

func (p *storedPreview) snapshot() ReorderPreview {
	return ReorderPreview{
		ID:              p.id,
		Rule:            p.rule,
		FromRule:        p.fromRule,
		BaselineVersion: p.baselineVersion,
		Order:           append([]string(nil), p.order...),
		Changes:         append([]PositionChange(nil), p.changes...),
	}
}

func (p *storedPreview) result() ReorderResult {
	return ReorderResult{ReorderPreview: p.snapshot(), NewVersion: p.newVersion}
}

func knownCriterion(c CriterionID) bool {
	switch c {
	case CriterionWaitTime, CriterionTier, CriterionScore:
		return true
	}
	return false
}

// sortQueueLocked re-ranks the waiting queue under the current rule.
func (w *Waitlist) sortQueueLocked() {
	sort.SliceStable(w.queue, func(i, j int) bool {
		return w.lessUnderRule(w.queue[i], w.queue[j], w.rule)
	})
}

func (w *Waitlist) lessUnderRule(a, b string, rule Rule) bool {
	aa, bb := w.apps[a], w.apps[b]
	for _, c := range rule.Criteria {
		va, vb := criterionValue(c, aa), criterionValue(c, bb)
		if va != vb {
			// All supported criteria rank smaller values first: higher
			// tier/score are stored negated and wait-time is chronological.
			return va < vb
		}
	}
	if aa.seq != bb.seq {
		return aa.seq < bb.seq
	}
	return aa.ID < bb.ID
}

func criterionValue(c CriterionID, app *Application) int64 {
	switch c {
	case CriterionTier:
		return int64(-app.Qualification.Tier)
	case CriterionScore:
		return int64(-app.Qualification.Score)
	case CriterionWaitTime:
		return app.JoinedAt.UnixNano()
	}
	return 0
}

func basisOf(q Qualification, joinedAt time.Time, seq int64, appID string, rule Rule) []string {
	basis := make([]string, 0, len(rule.Criteria)+1)
	for _, c := range rule.Criteria {
		switch c {
		case CriterionTier:
			basis = append(basis, fmt.Sprintf("%s=%d", c, q.Tier))
		case CriterionScore:
			basis = append(basis, fmt.Sprintf("%s=%d", c, q.Score))
		case CriterionWaitTime:
			basis = append(basis, fmt.Sprintf("%s=%d", c, joinedAt.UnixNano()))
		}
	}
	basis = append(basis, fmt.Sprintf("seq=%d", seq))
	return basis
}

func (w *Waitlist) positionChangesLocked(oldOrder, newOrder []string, rule Rule) []PositionChange {
	oldPos := make(map[string]int, len(oldOrder))
	for i, id := range oldOrder {
		oldPos[id] = i
	}
	var changes []PositionChange
	for newPos, id := range newOrder {
		oldPos := oldPos[id]
		if oldPos == newPos {
			continue
		}
		changes = append(changes, PositionChange{
			ApplicationID: id,
			From:          oldPos,
			To:            newPos,
			Reason:        w.changeReasonLocked(id, oldPos, newPos, oldOrder, newOrder, rule),
		})
	}
	return changes
}

func (w *Waitlist) changeReasonLocked(appID string, from, to int, oldOrder, newOrder []string, rule Rule) string {
	app := w.apps[appID]
	neighbour := func() string {
		if to < from {
			for _, other := range newOrder[to+1 : from+1] {
				if oldIndexOf(oldOrder, other) < from {
					return other
				}
			}
		} else {
			for _, other := range newOrder[from:to] {
				if oldIndexOf(oldOrder, other) > from {
					return other
				}
			}
		}
		return ""
	}
	otherID := neighbour()
	direction := "moved up"
	if to > from {
		direction = "moved down"
	}
	if otherID == "" {
		return direction + " under new rule ordering"
	}
	other := w.apps[otherID]
	for _, c := range rule.Criteria {
		va, vb := criterionValue(c, app), criterionValue(c, other)
		if va == vb {
			continue
		}
		if (to < from) == (va < vb) {
			return fmt.Sprintf("%s on criterion %s (%d vs %d of %s)",
				direction, c, rawCriterionValue(c, app), rawCriterionValue(c, other), otherID)
		}
	}
	return direction + " under new rule ordering"
}

func oldIndexOf(order []string, id string) int {
	for i, x := range order {
		if x == id {
			return i
		}
	}
	return -1
}

func rawCriterionValue(c CriterionID, app *Application) int {
	switch c {
	case CriterionTier:
		return app.Qualification.Tier
	case CriterionScore:
		return app.Qualification.Score
	case CriterionWaitTime:
		return int(app.JoinedAt.UnixNano())
	}
	return 0
}
