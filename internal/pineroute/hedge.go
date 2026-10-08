package pineroute

import (
	"context"
	"os"
	"time"

	"github.com/go-gost/core/chain"
)

// Hedged CONNECT: when the first route has not answered within the hedge
// delay, the router dials the next route in parallel and keeps whichever
// connects first. The coordinator sets PINE_GOST_HEDGE_DELAY from the
// deployment's chart value; unset or zero turns hedging off.
const hedgeDelayEnvironment = "PINE_GOST_HEDGE_DELAY"

// ReasonSlow ejects a route that kept losing hedges: other routes connected
// the same addresses while it had not answered.
const ReasonSlow = "slow"

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
// Only tests in this module use it.
func SetHedgeDelayForTest(delay time.Duration) (restore func()) {
	previous := hedgeDelay
	hedgeDelay = delay
	return func() { hedgeDelay = previous }
}

// NoteSlowSuspect records that node had not answered dialed for address when
// a hedge route connected it. Like a failed attempt, it is charged only if
// the winner reached the same dialed address (BlameSuspects).
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
// while its first route had not failed. A site's pinned route (#636) must not
// move for such a win: the first route only answered slowly.
func HedgeWon(ctx context.Context) bool {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hedgeWon
}
