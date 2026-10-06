package pineroute

import (
	"context"
	"errors"
	"net"
	"syscall"

	"github.com/go-gost/core/chain"
)

// Failure causes carried on Pine request events. GOST derives the causes it
// can prove from the attempts of one request. The session coordinator owns
// public DNS and refines site/unknown into address_invalid or not_public, so
// those two values never come from here.
const (
	CauseVendorPolicy    = "vendor_policy"
	CauseRoute           = "route"
	CauseIPRefusedBySite = "ip_refused_by_site"
	CauseNetwork         = "network"
	CauseSite            = "site"
	CauseUnknown         = "unknown"
)

// attemptRecord is the part of one route attempt the cause rules need.
type attemptRecord struct {
	route  routeKey
	dialed string
	failed bool
	// reply is the SOCKS5 reply code a proxy returned for the destination,
	// or 0 when the attempt failed for another reason.
	reply uint8
	// destination reports a failure that concerns the destination rather than
	// the route: a destination SOCKS reply, or a direct dial that reached the
	// network and was refused or could not resolve the name.
	destination bool
}

// RecordAttempt remembers the outcome of one route attempt of the request in
// ctx. dialed is the address sent in CONNECT.
func RecordAttempt(ctx context.Context, node *chain.Node, network, dialed string, err error) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	route := nodeRouteKey(node)
	if a == nil || route == (routeKey{}) {
		return
	}
	record := attemptRecord{route: route, dialed: network + "/" + dialed, failed: err != nil}
	if err != nil {
		var reply socks5ReplyError
		if !isUpstreamFailure(err) && errors.As(err, &reply) {
			record.reply = reply.SOCKS5ReplyCode()
		}
		record.destination = DestinationScoped(network, err) || IsDirect(node) && directDestinationFailure(err)
	}
	a.mu.Lock()
	a.records = append(a.records, record)
	a.mu.Unlock()
}

// directDestinationFailure reports a direct dial the destination itself
// answered: the name does not resolve, or the host refused the connection.
// Timeouts, resets and local network errors say nothing about the site.
func directDestinationFailure(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsNotFound
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

// FailureCause classifies the request in ctx after its last attempt. err is
// the request's final error. It returns "" when the first attempt succeeded.
func FailureCause(ctx context.Context, err error) string {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return ""
	}
	a.mu.Lock()
	records := append([]attemptRecord(nil), a.records...)
	a.mu.Unlock()
	return failureCause(records, err == nil, err != nil && RequestCanceled(ctx, err))
}

func failureCause(records []attemptRecord, succeeded, canceled bool) string {
	if succeeded {
		return failoverCause(records)
	}
	if canceled || len(records) == 0 {
		// The caller gave up, or every route was excluded before any attempt
		// (for example by the refusal cache): this request proves nothing.
		return CauseUnknown
	}
	routes := make(map[routeKey]struct{}, len(records))
	policyOnly, anyDestination, allTransport := true, false, true
	for _, record := range records {
		routes[record.route] = struct{}{}
		if record.reply != replyNotAllowed {
			policyOnly = false
		}
		if record.destination {
			allTransport = false
			if record.reply != replyNotAllowed {
				anyDestination = true
			}
		}
	}
	switch {
	case policyOnly:
		return CauseVendorPolicy
	case len(routes) < 2:
		return CauseUnknown
	case anyDestination:
		// At least one route reached its proxy and the destination still
		// failed there; no route reached the site.
		return CauseSite
	case allTransport:
		return CauseNetwork
	default:
		// Only policy refusals and transport failures: nothing shows the site
		// itself is unreachable.
		return CauseUnknown
	}
}

// failoverCause names why routes before the winner failed. Only a failure on
// the same dialed address the winner reached proves anything about the route;
// a failure on another address (a different resolution) stays unknown.
func failoverCause(records []attemptRecord) string {
	if len(records) == 0 || records[len(records)-1].failed {
		return ""
	}
	winner := records[len(records)-1]
	failedBefore := false
	for _, record := range records[:len(records)-1] {
		if !record.failed {
			continue
		}
		failedBefore = true
		if record.dialed != winner.dialed {
			continue
		}
		switch record.reply {
		case replyNotAllowed:
			return CauseVendorPolicy
		case 5:
			return CauseIPRefusedBySite
		case 3, 4, 6:
			return CauseRoute
		}
		return CauseNetwork
	}
	if failedBefore {
		return CauseUnknown
	}
	return ""
}
