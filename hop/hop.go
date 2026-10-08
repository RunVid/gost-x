package hop

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-gost/core/bypass"
	"github.com/go-gost/core/chain"
	"github.com/go-gost/core/hop"
	"github.com/go-gost/core/logger"
	"github.com/go-gost/core/routing"
	"github.com/go-gost/core/selector"
	"github.com/go-gost/x/config"
	node_parser "github.com/go-gost/x/config/parsing/node"
	"github.com/go-gost/x/internal/loader"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
)

type options struct {
	name        string
	nodes       []*chain.Node
	bypass      bypass.Bypass
	selector    selector.Selector[*chain.Node]
	fileLoader  loader.Loader
	redisLoader loader.Loader
	httpLoader  loader.Loader
	period      time.Duration
	logger      logger.Logger
	affinity    *pineroute.Affinity
}

type Option func(*options)

func NameOption(name string) Option {
	return func(o *options) {
		o.name = name
	}
}

func NodeOption(nodes ...*chain.Node) Option {
	return func(o *options) {
		o.nodes = nodes
	}
}
func BypassOption(bp bypass.Bypass) Option {
	return func(o *options) {
		o.bypass = bp
	}
}

func SelectorOption(s selector.Selector[*chain.Node]) Option {
	return func(o *options) {
		o.selector = s
	}
}

func ReloadPeriodOption(period time.Duration) Option {
	return func(opts *options) {
		opts.period = period
	}
}

func FileLoaderOption(fileLoader loader.Loader) Option {
	return func(opts *options) {
		opts.fileLoader = fileLoader
	}
}

func RedisLoaderOption(redisLoader loader.Loader) Option {
	return func(opts *options) {
		opts.redisLoader = redisLoader
	}
}

func HTTPLoaderOption(httpLoader loader.Loader) Option {
	return func(opts *options) {
		opts.httpLoader = httpLoader
	}
}

// AffinityOption turns on Pine per-site route affinity for the hop.
func AffinityOption(affinity *pineroute.Affinity) Option {
	return func(opts *options) {
		opts.affinity = affinity
	}
}

func LoggerOption(logger logger.Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}

type chainHop struct {
	nodes      []*chain.Node
	options    options
	logger     logger.Logger
	mu         sync.RWMutex
	cancelFunc context.CancelFunc
}

func NewHop(opts ...Option) hop.Hop {
	var options options
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	ctx, cancel := context.WithCancel(context.TODO())
	p := &chainHop{
		nodes:      options.nodes,
		cancelFunc: cancel,
		options:    options,
		logger:     options.logger,
	}

	if p.logger == nil {
		p.logger = xlogger.Nop()
	}

	go p.periodReload(ctx)

	return p
}

func (p *chainHop) Nodes() []*chain.Node {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nodes
}

func (p *chainHop) Select(ctx context.Context, opts ...hop.SelectOption) *chain.Node {
	var options hop.SelectOptions
	for _, opt := range opts {
		opt(&options)
	}

	log := p.logger

	// hop level bypass
	if p.options.bypass != nil &&
		p.options.bypass.Contains(ctx, options.Network, options.Addr, bypass.WithHostOpton(options.Host)) {
		return nil
	}

	all := p.Nodes()
	pineroute.NoteHop(ctx, all)
	site := pineroute.Site(ctx, options.Network, options.Host, p.options.affinity)
	if site != nil && hasMatcher(all) {
		// A matcher's priority would override the pin (loaders can add
		// matcher nodes after parsing); such hops stay plain fifo.
		site = nil
	}
	var nodes []*chain.Node
	var pinned *chain.Node
	pinnedOffered := false
	// eligible counts nodes this destination may use at all (bypass, matcher
	// and filter applied); refused counts those its refusals exclude.
	eligible, refused := 0, 0
	for _, node := range all {
		if node == nil {
			continue
		}
		if site != nil && p.offered(ctx, node, &options) {
			// The first node offered for this host is the site's default
			// route, whatever its transient state; it needs no pin.
			site.SetDefault(node)
		}
		if site.IsPinned(node) {
			// Judged by selectPinned below, which may hold a login site on
			// it through a transient refusal.
			pinned, pinnedOffered = node, p.offered(ctx, node, &options)
			if pinnedOffered {
				eligible++
				if pineroute.Refused(ctx, node, options.Network, options.Host) {
					refused++
				}
			}
			continue
		}
		if !pineroute.Escalation && pineroute.Skip(ctx, node, options.Network, options.Host) {
			continue
		}
		// node level bypass
		if node.Options().Bypass != nil &&
			node.Options().Bypass.Contains(ctx, options.Network, options.Addr, bypass.WithHostOpton(options.Host)) {
			continue
		}

		if matcher := node.Options().Matcher; matcher != nil {
			req := routing.Request{
				ClientIP: options.ClientIP,
				Host:     options.Host,
				Protocol: options.Protocol,
				Method:   options.Method,
				Path:     options.Path,
				Query:    options.Query,
				Header:   options.Header,
			}
			if !matcher.Match(&req) {
				continue
			}
			log.Debugf("node %s match request %s %s, priority %d", node.Name, req.Protocol, req.Host, node.Options().Priority)
		} else {
			if !p.isEligible(node, &options) {
				continue
			}
		}

		eligible++
		if pineroute.Skip(ctx, node, options.Network, options.Host) {
			if pineroute.Refused(ctx, node, options.Network, options.Host) {
				refused++
			}
			continue
		}
		nodes = append(nodes, node)
	}
	if site.Pinned() {
		node, done, keep := p.selectPinned(ctx, site, pinned, pinnedOffered, &options)
		if done {
			return node
		}
		if keep {
			nodes = append(nodes, pinned)
		}
	}
	if len(nodes) == 0 && eligible > 0 && refused == eligible {
		pineroute.NoteAllRefused(ctx)
	}
	if preferred := pineroute.WithoutEjected(nodes); len(preferred) > 0 {
		if node := p.selectPreferred(ctx, preferred); node != nil {
			return node
		}
	}
	if node := p.selectNode(ctx, nodes); node != nil {
		return node
	}
	return pineroute.PanicSelect(ctx, nodes)
}

func hasMatcher(nodes []*chain.Node) bool {
	for _, node := range nodes {
		if node != nil && node.Options().Matcher != nil {
			return true
		}
	}
	return false
}

// offered reports whether the hop offers node for this request's host: its
// node-level bypass and filter (affinity is off on hops with matchers).
func (p *chainHop) offered(ctx context.Context, node *chain.Node, options *hop.SelectOptions) bool {
	return (node.Options().Bypass == nil ||
		!node.Options().Bypass.Contains(ctx, options.Network, options.Addr, bypass.WithHostOpton(options.Host))) &&
		p.isEligible(node, options)
}

// selectPinned decides whether the request uses its site's pinned route.
// done means Select returns node: the pinned route, or nil when a held login
// site must fail instead of moving. Otherwise the request continues in fifo
// order, with the pinned route kept as a candidate when keep is set. A
// reason recorded with Move lets the router re-pin on success; without one
// the request is a detour that leaves the pin in place. Precedence: removed,
// not offered, outside Chrome's timezone, ejected, failed in this request,
// refused, cooling down.
func (p *chainHop) selectPinned(ctx context.Context, site *pineroute.SiteSelection, pinned *chain.Node, offered bool, options *hop.SelectOptions) (node *chain.Node, done, keep bool) {
	switch {
	case pinned == nil:
		site.Move(pineroute.MoveRouteRemoved)
		return nil, false, false
	case !offered:
		// Not offered for this host: serve it elsewhere, keep the pin.
		return nil, false, false
	case !site.PinAllowed(pinned):
		// Its exit is no longer in Chrome's timezone (requalified, or the
		// plan lost the timezone): drop the pin, keep the route in fifo.
		site.Move(pineroute.MoveRouteRemoved)
		return nil, false, !pineroute.Skip(ctx, pinned, options.Network, options.Host)
	case pineroute.Ejected(pinned):
		// Route-wide ejection moves every site, login sites included. The
		// route stays a last resort unless this request cannot use it.
		site.Move(pineroute.MoveRouteEjected)
		return nil, false, !pineroute.Skip(ctx, pinned, options.Network, options.Host)
	case pineroute.Tried(ctx, pinned):
		// Failed in this request: AffinityAttempt decided to stop, move or
		// detour.
		return nil, site.Stopped(), false
	}
	if refused, policy := pineroute.RefusedFor(pinned, options.Network, options.Host); refused {
		if policy || !site.Hold() {
			site.Move(pineroute.MoveSiteRefused)
			return nil, false, false
		}
		return pinned, true, false
	}
	// The pair is judged by the selector's filters like any candidate set:
	// the cooldown applies, and backup suppression does not (both copies are
	// backups or neither is), so a site pinned to a fallback stays on it.
	if p.selectNode(ctx, []*chain.Node{pinned, pinned.Copy()}) == nil {
		// Cooling down after a route failure: login sites wait for it,
		// other sites detour until it recovers.
		if site.HoldRouteFailure() {
			return pinned, true, false
		}
		return nil, false, true
	}
	return pinned, true, false
}

// selectPreferred selects among non-ejected nodes with the hop's selector.
// Selection and the selector's filters pass a single candidate through
// unchecked, so a lone node is offered together with a copy, which shares its
// marker and metadata, and is judged by the same cooldown rules as any other.
func (p *chainHop) selectPreferred(ctx context.Context, preferred []*chain.Node) *chain.Node {
	if len(preferred) > 1 {
		return p.selectNode(ctx, preferred)
	}
	only := preferred[0]
	if p.selectNode(ctx, []*chain.Node{only, only.Copy()}) == nil {
		return nil
	}
	return only
}

func (p *chainHop) selectNode(ctx context.Context, nodes []*chain.Node) *chain.Node {
	if len(nodes) == 0 {
		return nil
	}
	if len(nodes) == 1 {
		return nodes[0]
	}

	// Stable: the caller's order (fifo, or a site's rendezvous order) must
	// survive among equal priorities.
	sort.SliceStable(nodes, func(i, j int) bool {
		return nodes[i].Options().Priority > nodes[j].Options().Priority
	})

	if nodes[0].Options().Priority > 0 {
		return nodes[0]
	}

	if s := p.options.selector; s != nil {
		return s.Select(ctx, nodes...)
	}
	return nodes[0]
}

func (p *chainHop) isEligible(node *chain.Node, opts *hop.SelectOptions) bool {
	if node == nil {
		return false
	}
	if node.Options().Filter == nil {
		return true
	}

	if !p.checkHost(opts.Host, node) || !p.checkProtocol(opts.Protocol, node) || !p.checkPath(opts.Path, node) {
		return false
	}
	return true
}

func (p *chainHop) checkHost(host string, node *chain.Node) bool {
	var vhost string
	if filter := node.Options().Filter; filter != nil {
		vhost = filter.Host
	}
	if vhost == "" { // backup node
		return true
	}

	if host == "" {
		return false
	}

	if v, _, _ := net.SplitHostPort(host); v != "" {
		host = v
	}

	if vhost == host || vhost[0] == '.' && strings.HasSuffix(host, vhost[1:]) {
		return true
	}

	return false
}

func (p *chainHop) checkProtocol(protocol string, node *chain.Node) bool {
	var prot string
	if filter := node.Options().Filter; filter != nil {
		prot = filter.Protocol
	}
	if prot == "" {
		return true
	}
	return prot == protocol
}

func (p *chainHop) checkPath(path string, node *chain.Node) bool {
	var pathFilter string
	if filter := node.Options().Filter; filter != nil {
		pathFilter = filter.Path
	}

	if pathFilter == "" {
		return true
	}

	return strings.HasPrefix(path, pathFilter)
}

func (p *chainHop) periodReload(ctx context.Context) error {
	if err := p.reload(ctx); err != nil {
		p.logger.Warnf("reload: %v", err)
	}

	period := p.options.period
	if period <= 0 {
		return nil
	}
	if period < time.Second {
		period = time.Second
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.reload(ctx); err != nil {
				p.logger.Warnf("reload: %v", err)
				// return err
			}
			p.logger.Debug("hop reload done")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *chainHop) reload(ctx context.Context) (err error) {
	nodes := p.options.nodes

	nl, err := p.load(ctx)

	nodes = append(nodes, nl...)

	p.logger.Debugf("load items %d", len(nodes))

	p.mu.Lock()
	defer p.mu.Unlock()

	p.nodes = nodes

	return
}

func (p *chainHop) load(ctx context.Context) (nodes []*chain.Node, err error) {
	if loader := p.options.fileLoader; loader != nil {
		r, er := loader.Load(ctx)
		if er != nil {
			p.logger.Warnf("file loader: %v", er)
		}
		nodes, _ = p.parseNode(r)
	}

	if loader := p.options.redisLoader; loader != nil {
		r, er := loader.Load(ctx)
		if er != nil {
			p.logger.Warnf("redis loader: %v", er)
		}
		ns, _ := p.parseNode(r)
		nodes = append(nodes, ns...)
	}

	if loader := p.options.httpLoader; loader != nil {
		r, er := loader.Load(ctx)
		if er != nil {
			p.logger.Warnf("http loader: %v", er)
		}
		if ns, _ := p.parseNode(r); ns != nil {
			nodes = append(nodes, ns...)
		}
	}

	return
}

func (p *chainHop) parseNode(r io.Reader) ([]*chain.Node, error) {
	if r == nil {
		return nil, nil
	}

	var ncs []*config.NodeConfig
	if err := json.NewDecoder(r).Decode(&ncs); err != nil {
		return nil, err
	}

	var nodes []*chain.Node
	for _, nc := range ncs {
		if nc == nil {
			continue
		}

		node, err := node_parser.ParseNode(p.options.name, nc, logger.Default())
		if err != nil {
			return nodes, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (p *chainHop) Close() error {
	p.cancelFunc()
	if p.options.fileLoader != nil {
		p.options.fileLoader.Close()
	}
	if p.options.redisLoader != nil {
		p.options.redisLoader.Close()
	}
	return nil
}

// AffinityForTest returns the hop's affinity configuration. For tests.
func (p *chainHop) AffinityForTest() *pineroute.Affinity { return p.options.affinity }
