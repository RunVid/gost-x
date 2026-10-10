package pineroute

import (
	"context"
	"time"

	"github.com/go-gost/core/chain"
)

// exclusion is why hop selection skipped a plan node for a request.
type exclusion uint8

const (
	excludedNone exclusion = iota
	// excludedPolicy and excludedTransient are refusal cache entries of the
	// policy (reply 2) and transient (reply 3/4/5/6/8) classes.
	excludedPolicy
	excludedTransient
	// excludedCooldown is the selector's failed-node marker (FailFilter).
	excludedCooldown
	// excludedEjected is a route ejection.
	excludedEjected
)

// Excluded counts the plan nodes a request never attempted, by the reason
// hop selection last skipped each of them for.
type Excluded struct {
	Policy    int `json:"policy,omitempty"`
	Transient int `json:"transient,omitempty"`
	Cooldown  int `json:"cooldown,omitempty"`
	Ejected   int `json:"ejected,omitempty"`
}

func noteExcluded(ctx context.Context, reason exclusion, nodes ...*chain.Node) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil || len(nodes) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.excluded == nil {
		a.excluded = map[routeKey]exclusion{}
	}
	for _, node := range nodes {
		if id := nodeRouteKey(node); id != (routeKey{}) {
			a.excluded[id] = reason
		}
	}
}

// NoteRefusal records that node was skipped for the request in ctx if its
// refusal cache entry for network/address excludes it.
func NoteRefusal(ctx context.Context, node *chain.Node, network, address string) {
	id := nodeRouteKey(node)
	if !Enabled || id == (routeKey{}) || !Tracking(ctx) {
		return
	}
	class, ok := defaultRefusals.lookup(id, network, address, time.Now())
	if !ok {
		return
	}
	reason := excludedTransient
	if class == classPolicy {
		reason = excludedPolicy
	}
	noteExcluded(ctx, reason, node)
}

// NoteEjected records that nodes were skipped for the request in ctx because
// they are ejected.
func NoteEjected(ctx context.Context, nodes ...*chain.Node) {
	noteExcluded(ctx, excludedEjected, nodes...)
}

// NoteCooldown records that the hop selector's failure cooldown dropped nodes
// for the request in ctx.
func NoteCooldown(ctx context.Context, nodes ...*chain.Node) {
	noteExcluded(ctx, excludedCooldown, nodes...)
}

// Exclusions returns the counts of plan nodes the request in ctx skipped and
// never attempted, or nil if there are none.
func Exclusions(ctx context.Context) *Excluded {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return nil
	}
	a.mu.Lock()
	excluded := untriedLocked(a)
	a.mu.Unlock()
	var counts Excluded
	for _, reason := range excluded {
		switch reason {
		case excludedPolicy:
			counts.Policy++
		case excludedTransient:
			counts.Transient++
		case excludedCooldown:
			counts.Cooldown++
		case excludedEjected:
			counts.Ejected++
		}
	}
	if counts == (Excluded{}) {
		return nil
	}
	return &counts
}

// untriedLocked returns the exclusions of the nodes a never attempted.
func untriedLocked(a *attempts) map[routeKey]exclusion {
	excluded := make(map[routeKey]exclusion, len(a.excluded))
	for id, reason := range a.excluded {
		if _, ok := a.tried[id]; !ok {
			excluded[id] = reason
		}
	}
	return excluded
}

// exclusionCause classifies a failed request the attempts alone leave
// unknown, from what happened to every plan node: attempted (records) or
// skipped (excluded, untried nodes only).
func exclusionCause(records []attemptRecord, plan map[routeKey]struct{}, excluded map[routeKey]exclusion) string {
	if len(plan) == 0 {
		return CauseUnknown
	}
	// destination maps a route that failed the destination to whether every
	// such failure was a policy refusal. Address-family replies (8) concern
	// one family only and count as neither destination nor route health.
	destination := map[routeKey]bool{}
	health := map[routeKey]struct{}{}
	for _, record := range records {
		switch {
		case !record.failed || record.reply == 8:
		case record.destination:
			policy, seen := destination[record.route]
			destination[record.route] = (policy || !seen) && record.reply == replyNotAllowed
		default:
			health[record.route] = struct{}{}
		}
	}
	covered, policyOnly := true, true
	refused, routeHealth := len(destination) > 0, len(health) > 0
	for route := range plan {
		if policy, ok := destination[route]; ok {
			policyOnly = policyOnly && policy
			continue
		}
		switch excluded[route] {
		case excludedPolicy:
		case excludedTransient:
			policyOnly = false
		default:
			covered = false
		}
	}
	for _, reason := range excluded {
		switch reason {
		case excludedPolicy, excludedTransient:
			refused = true
		case excludedCooldown, excludedEjected:
			routeHealth = true
		}
	}
	switch {
	case covered && policyOnly:
		return CauseVendorPolicy
	case covered:
		return CauseSite
	case !refused && routeHealth:
		return CauseNetwork
	}
	return CauseUnknown
}
