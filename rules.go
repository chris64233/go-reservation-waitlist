package goreservationwaitlist

import (
	"fmt"
	"sort"
	"time"
)

// Sortable condition fields. Every condition is evaluated strictly in the
// order listed by the rule set; the first unequal condition decides.
const (
	FieldEligible = "eligible" // eligible applicants first
	FieldPriority = "priority" // larger priority first
	FieldScore    = "score"    // larger score first
	FieldMember   = "member"   // flagged members first
	FieldJoined   = "joined"   // earlier JoinedAt first
	FieldSeq      = "seq"      // earlier enqueue first (stable tie break)
	FieldID       = "id"       // lexicographic application id
)

// Condition is one ordered rule clause. Desc reverses the natural direction
// of the field; the natural directions are documented on the field constants.
type Condition struct {
	Field string
	Desc  bool
}

// RuleSet is an immutable, versioned ordering rule. Once installed it never
// changes; publishing a new rule installs a new version.
type RuleSet struct {
	Version    int64
	Conditions []Condition
	Note       string
	CreatedAt  time.Time
}

// DefaultRules returns the FIFO rule used by a new waitlist.
func DefaultRules() RuleSet {
	return RuleSet{Conditions: []Condition{{Field: FieldSeq}}}
}

// PromotionBasis records why an application won a slot: its position under
// the rule version that was active when the offer was made.
type PromotionBasis struct {
	Position    int
	RuleVersion int64
	Reasons     []string
}

// ReorderRequest prepares a reorder under a new immutable rule version. ID
// is the idempotency key of the whole prepare/publish cycle.
type ReorderRequest struct {
	ID         string
	Conditions []Condition
	Note       string
}

// PositionChange describes one application's movement in a reorder preview
// or in the last published queue version.
type PositionChange struct {
	ApplicationID string
	From          int
	To            int
	Reason        string
}

// ReorderPreview is the immutable preview computed for one reorder request
// against one queue/rule baseline.
type ReorderPreview struct {
	ID               string
	Rule             RuleSet
	BaseQueueVersion int64
	BaseRuleVersion  int64
	PreparedAt       time.Time
	Order            []string
	Changes          []PositionChange
}

// storedPreview keeps a prepared preview plus its validity flag. Stale
// previews are retained so that replays can still distinguish an unchanged
// request (ErrStaleVersion) from changed conditions (ErrConflict); they are
// invisible to Preview/Publish and cannot be applied in part.
type storedPreview struct {
	preview ReorderPreview
	stale   bool
}

// InstallRules registers an immutable rule set and returns it with its
// assigned version. The active queue order does not change until a reorder
// with the new rule is prepared and published.
func (w *Waitlist) InstallRules(conditions []Condition, note string, now time.Time) (RuleSet, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.installRulesLocked(conditions, note, now), nil
}

// PrepareReorder computes a reorder preview for all applications that have
// not yet been promoted. Promotions inside their reservation window are
// untouched: their slots and offers cannot be revoked by a reorder.
//
// The call is idempotent on req.ID: the same id with the same conditions
// against the same baseline returns the stored preview; different conditions
// return ErrConflict; a drifted queue baseline returns ErrStaleVersion.
func (w *Waitlist) PrepareReorder(req ReorderRequest, now time.Time) (ReorderPreview, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(req.ID) == 0 {
		return ReorderPreview{}, ErrNotFound
	}
	if existing, ok := w.previews[req.ID]; ok {
		if !conditionsEqual(existing.preview.Rule.Conditions, normalizeConditions(req.Conditions)) {
			return ReorderPreview{}, ErrConflict
		}
		if existing.stale ||
			existing.preview.BaseQueueVersion != w.queueVersion ||
			existing.preview.BaseRuleVersion != w.activeRule {
			return ReorderPreview{}, ErrStaleVersion
		}
		return existing.preview, nil
	}
	rule := w.installRulesLocked(req.Conditions, req.Note, now)
	preview := w.buildPreviewLocked(req.ID, rule, now)
	w.previews[req.ID] = &storedPreview{preview: preview}
	return preview, nil
}

// Preview returns a previously prepared reorder preview. It fails with
// ErrPreviewGone if the preview never existed or was invalidated.
func (w *Waitlist) Preview(id string) (ReorderPreview, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.previews[id]
	if !ok || p.stale {
		return ReorderPreview{}, ErrPreviewGone
	}
	return p.preview, nil
}

// PublishReorder atomically publishes the queue version prepared earlier.
// Any join, withdrawal, promotion, decline, slot release or other baseline
// change since preparation invalidates the preview (ErrPreviewGone), in
// which case nothing is applied; a fresh preview must be prepared. Offers
// already made stay exactly as they were, so no slot is silently handed to
// somebody else.
func (w *Waitlist) PublishReorder(id string, now time.Time) (ReorderPreview, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry, ok := w.previews[id]
	if !ok {
		return ReorderPreview{}, ErrPreviewGone
	}
	preview := entry.preview
	if entry.stale || preview.BaseQueueVersion != w.queueVersion || preview.BaseRuleVersion != w.activeRule {
		delete(w.previews, id)
		return ReorderPreview{}, ErrPreviewGone
	}

	ordered := make([]*Application, 0, len(preview.Order))
	for _, appID := range preview.Order {
		if app, ok := w.apps[appID]; ok && app.Status == StatusWaiting {
			ordered = append(ordered, app)
		}
	}
	w.queue = w.queue[:0]
	for _, app := range ordered {
		w.queue = append(w.queue, app.ID)
	}
	w.activeRule = preview.Rule.Version
	w.lastChanges = append([]PositionChange(nil), preview.Changes...)
	w.queueVersion++
	w.version++
	// Offers inside their reservation window stay valid: carry their
	// staleness guard forward to the new waitlist version.
	for _, promoID := range w.order {
		if p := w.promotions[promoID]; p.State == PromotionPending {
			p.Version = w.version
		}
	}
	delete(w.previews, id)
	// Any other pending preview was prepared against a different baseline.
	w.invalidatePreviewsLocked()
	return preview, nil
}

func (w *Waitlist) buildPreviewLocked(id string, rule RuleSet, now time.Time) ReorderPreview {
	oldOrder := append([]string(nil), w.queue...)
	oldPos := make(map[string]int, len(oldOrder))
	for i, appID := range oldOrder {
		oldPos[appID] = i + 1
	}

	apps := make([]*Application, 0, len(w.queue))
	for _, appID := range w.queue {
		apps = append(apps, w.apps[appID])
	}
	sort.SliceStable(apps, func(i, j int) bool {
		return w.compareLocked(apps[i], apps[j], rule) < 0
	})

	preview := ReorderPreview{
		ID:               id,
		Rule:             rule,
		BaseQueueVersion: w.queueVersion,
		BaseRuleVersion:  w.activeRule,
		PreparedAt:       now,
		Order:            make([]string, 0, len(apps)),
	}
	for newPos, app := range apps {
		preview.Order = append(preview.Order, app.ID)
		from := oldPos[app.ID]
		to := newPos + 1
		if from == to {
			continue
		}
		preview.Changes = append(preview.Changes, PositionChange{
			ApplicationID: app.ID,
			From:          from,
			To:            to,
			Reason:        w.changeReasonLocked(app, from, to, oldOrder, apps, rule),
		})
	}
	return preview
}

func (w *Waitlist) changeReasonLocked(app *Application, from, to int, oldOrder []string, newOrder []*Application, rule RuleSet) string {
	var crossedID string
	if to < from {
		for pos := from - 2; pos >= to-1; pos-- {
			crossedID = oldOrder[pos]
		}
	} else {
		crossedID = newOrder[to-2].ID
	}
	crossed := w.apps[crossedID]
	for _, cond := range rule.Conditions {
		if c := compareField(app, crossed, cond); c != 0 {
			return fmt.Sprintf("%s: %s=%s vs %s=%s",
				cond.Field, app.ID, fieldValue(app, cond.Field),
				crossedID, fieldValue(crossed, cond.Field))
		}
	}
	return "seq: earlier enqueue"
}

func (w *Waitlist) installRulesLocked(conditions []Condition, note string, now time.Time) RuleSet {
	version := int64(1)
	if len(w.ruleVersions) > 0 {
		version = w.ruleVersions[len(w.ruleVersions)-1] + 1
	}
	rule := RuleSet{
		Version:    version,
		Conditions: normalizeConditions(conditions),
		Note:       note,
		CreatedAt:  now,
	}
	w.rules[version] = &rule
	w.ruleVersions = append(w.ruleVersions, version)
	return rule
}

func (w *Waitlist) installRuleLocked(rule RuleSet) RuleSet {
	installed := w.installRulesLocked(rule.Conditions, rule.Note, rule.CreatedAt)
	w.activeRule = installed.Version
	return installed
}

func (w *Waitlist) activeRuleLocked() *RuleSet {
	return w.rules[w.activeRule]
}

// insertOrderedLocked places a freshly joined application at the position
// dictated by the active rule, keeping same-rule order stable.
func (w *Waitlist) insertOrderedLocked(app *Application) {
	rule := *w.activeRuleLocked()
	pos := sort.Search(len(w.queue), func(i int) bool {
		return w.compareLocked(app, w.apps[w.queue[i]], rule) < 0
	})
	w.queue = append(w.queue, "")
	copy(w.queue[pos+1:], w.queue[pos:])
	w.queue[pos] = app.ID
}

func (w *Waitlist) compareLocked(a, b *Application, rule RuleSet) int {
	for _, cond := range rule.Conditions {
		if c := compareField(a, b, cond); c != 0 {
			return c
		}
	}
	return 0
}

func compareField(a, b *Application, cond Condition) int {
	c := 0
	switch cond.Field {
	case FieldEligible:
		c = boolRank(b.Profile.Eligible) - boolRank(a.Profile.Eligible)
	case FieldPriority:
		c = b.Profile.Priority - a.Profile.Priority
	case FieldScore:
		c = b.Profile.Score - a.Profile.Score
	case FieldMember:
		c = boolRank(b.Profile.Member) - boolRank(a.Profile.Member)
	case FieldJoined:
		c = compareTime(a.JoinedAt, b.JoinedAt)
	case FieldSeq:
		c = compareInt64(a.Seq, b.Seq)
	case FieldID:
		switch {
		case a.ID < b.ID:
			c = -1
		case a.ID > b.ID:
			c = 1
		}
	}
	if c != 0 && cond.Desc {
		c = -c
	}
	return c
}

func boolRank(v bool) int {
	if v {
		return 1
	}
	return 0
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareTime(a, b time.Time) int {
	switch {
	case a.Before(b):
		return -1
	case a.After(b):
		return 1
	default:
		return 0
	}
}

func fieldValue(app *Application, field string) string {
	switch field {
	case FieldEligible:
		return fmt.Sprintf("%t", app.Profile.Eligible)
	case FieldPriority:
		return fmt.Sprintf("%d", app.Profile.Priority)
	case FieldScore:
		return fmt.Sprintf("%d", app.Profile.Score)
	case FieldMember:
		return fmt.Sprintf("%t", app.Profile.Member)
	case FieldJoined:
		return app.JoinedAt.Format(time.RFC3339)
	case FieldSeq:
		return fmt.Sprintf("%d", app.Seq)
	default:
		return app.ID
	}
}

func (w *Waitlist) promotionBasisLocked(app *Application) PromotionBasis {
	rule := w.activeRuleLocked()
	reasons := make([]string, 0, len(rule.Conditions))
	for _, cond := range rule.Conditions {
		reasons = append(reasons, fmt.Sprintf("%s=%s", cond.Field, fieldValue(app, cond.Field)))
	}
	return PromotionBasis{
		Position:    1,
		RuleVersion: rule.Version,
		Reasons:     reasons,
	}
}

func (w *Waitlist) invalidatePreviewsLocked() {
	for id := range w.previews {
		w.previews[id].stale = true
	}
}

// normalizeConditions guarantees a stable total order: the global enqueue
// sequence is always the final tie break.
func normalizeConditions(conditions []Condition) []Condition {
	normalized := make([]Condition, 0, len(conditions)+1)
	seen := map[string]bool{}
	for _, cond := range conditions {
		if cond.Field == "" || seen[cond.Field] {
			continue
		}
		seen[cond.Field] = true
		normalized = append(normalized, cond)
	}
	if !seen[FieldSeq] {
		normalized = append(normalized, Condition{Field: FieldSeq})
	}
	return normalized
}

func conditionsEqual(a, b []Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
