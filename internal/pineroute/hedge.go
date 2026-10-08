package pineroute

import (
	"context"
	"os"
	"time"

	"github.com/go-gost/core/chain"
)

// Hedged CONNECT: when the first route has not answered within the hedge
// delay, the router dials the next route of the same source list in parallel
// and keeps whichever connects first. Unset or zero turns hedging off.
const hedgeDelayEnvironment = "PINE_GOST_HEDGE_DELAY"

// ReasonSlow ejects a route that lost hedges for slowHosts distinct hosts
// within escalationWindow. A lost hedge is weaker evidence than a refusal
// another route got past, so the bar is higher than escalationHosts.
const (
	ReasonSlow = "slow"
	slowHosts  = 6
)

var hedgeDelay = hedgeDelayFromEnvironment()

func hedgeDelayFromEnvironment() time.Duration {
	delay, err := time.ParseDuration(os.Getenv(hedgeDelayEnvironment))
	if err != nil || delay <= 0 {
		return 0
	}
	return delay
}

// HedgeDelay returns how long the router waits for the first route before
// it dials a second one, or 0 when hedging is off.
func HedgeDelay() time.Duration {
	if !Enabled {
		return 0
	}
	return hedgeDelay
}

// SetHedgeDelayForTest sets the hedge delay until the returned function runs.
func SetHedgeDelayForTest(delay time.Duration) (restore func()) {
	previous := hedgeDelay
	hedgeDelay = delay
	return func() { hedgeDelay = previous }
}

// HedgeCandidate reports whether node may hedge for first: a managed route of
// the same source list, so a hedge win does not move a site to another exit
// pool mid-session.
func HedgeCandidate(first, node *chain.Node) bool {
	if first == nil || node == nil || IsDirect(node) {
		return false
	}
	return nodeRouteInfo(first).sourceListID == nodeRouteInfo(node).sourceListID
}

// NoteSlowSuspect records that node had not answered dialed for address when
// a hedge route connected it. It is charged only if the winner reached the
// same dialed address (BlameSuspects).
func NoteSlowSuspect(ctx context.Context, node *chain.Node, network, address, dialed string) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	route, ok := managedIncarnation(node)
	host := destinationHost(address)
	if a == nil || !ok || host == "" || dialed == "" || !tcpNetwork(network) || a.request.Err() != nil {
		return
	}
	a.mu.Lock()
	a.suspects = append(a.suspects, suspect{route: route, info: nodeRouteInfo(node), host: host,
		dialed: network + "/" + dialed, at: time.Now(), slow: true})
	a.mu.Unlock()
}

// MarkHedgeWon records that the request in ctx was served by its hedge route
// while its first route had not failed.
func MarkHedgeWon(ctx context.Context) {
	if a, _ := ctx.Value(attemptsKey{}).(*attempts); a != nil {
		a.mu.Lock()
		a.hedgeWon = true
		a.mu.Unlock()
	}
}

// HedgeWon reports whether the request in ctx was served by a hedge route
// while its first route had not failed, so site affinity keeps its pin.
func HedgeWon(ctx context.Context) bool {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hedgeWon
}
