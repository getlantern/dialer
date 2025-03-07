package dialer

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

type connectTimeProxyDialer struct {
	ProxyDialer

	connectTime time.Duration
}

type connectedDialers struct {
	dialers []connectTimeProxyDialer
	sync.RWMutex
}

// fastConnectDialer stores the time it took to connect to each dialer and uses
// that information to select the fastest dialer to use.
type fastConnectDialer struct {
	topDialer protectedDialer
	connected connectedDialers

	next func(*Options, Dialer) Dialer
	opts *Options

	// Create a channel for stopping connections to dialers
	stopCh chan struct{}
}

// Make sure fastConnectDialer implements Dialer
var _ Dialer = (*fastConnectDialer)(nil)

func newFastConnectDialer(opts *Options, next func(opts *Options, existing Dialer) Dialer) *fastConnectDialer {
	if opts.OnError == nil {
		opts.OnError = func(error, bool) {}
	}
	if opts.OnSuccess == nil {
		opts.OnSuccess = func(ProxyDialer) {}
	}
	return &fastConnectDialer{
		connected: connectedDialers{
			dialers: make([]connectTimeProxyDialer, 0),
		},
		opts:      opts,
		next:      next,
		topDialer: protectedDialer{},
		stopCh:    make(chan struct{}, 10),
	}
}

func (fcd *fastConnectDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// Use the dialer with the lowest connect time
	td := fcd.topDialer.get()
	if td == nil {
		return nil, fmt.Errorf("no top dialer")
	}

	// Note that we don't currently check if the dialer supports
	// the domain here.
	conn, failedUpstream, err := td.DialContext(ctx, network, addr)
	if err != nil {
		hasSucceeding := len(fcd.connected.dialers) > 0
		fcd.opts.OnError(err, hasSucceeding)
		// Error connecting to the proxy or to the destination
		if failedUpstream {
			// Error connecting to the destination
			log.Debugf("Error connecting to upstream destination %v: %v", addr, err)
		} else {
			// Error connecting to the proxy
			log.Debugf("Error connecting to proxy %v: %v", td.Name(), err)
		}
		return nil, err
	}
	fcd.opts.OnSuccess(td)
	return conn, err
}

func (fcd *fastConnectDialer) Close() {
	// We don't call Stop on the Dialers themselves here because they are likely
	// in use by other Dialers, such as the BanditDialer.
	// Stop all dialing
	fcd.stopCh <- struct{}{}
}

func (fcd *fastConnectDialer) onConnected(pd ProxyDialer, connectTime time.Duration) {
	log.Debugf("Connected to %v", pd.Name())

	newTopDialer := fcd.connected.onConnected(pd, connectTime)

	// Set top dialer if the fastest dialer changed.
	td := fcd.topDialer.get()
	if td != newTopDialer {
		log.Debugf("Setting new top dialer to %v", newTopDialer.Name())
		fcd.topDialer.set(newTopDialer)
	}
	fcd.opts.OnSuccess(fcd.topDialer.get())
}

// connectAll dials all the dialers in parallel to connect the user as quickly as
// possible on startup.
func (fcd *fastConnectDialer) connectAll(dialers []ProxyDialer) {
	if len(dialers) == 0 {
		log.Errorf("No dialers to connect to")
		return
	}
	log.Debugf("Dialing all dialers in parallel %#v", dialers)
	// Create a channel for connected dialers
	connectedCh := make(chan connectTimeProxyDialer, len(dialers))

	// Connect to all dialers in parallel
	fcd.parallelDial(dialers, connectedCh)
	// Wait for at least one dialer to connect
	select {
	case <-fcd.stopCh:
		log.Debug("Stopping parallel dialing")
		return
	case <-time.After(10 * time.Second):
		log.Debug("Timed out waiting for dialers to connect")
		return
	case ctd := <-connectedCh:
		fcd.onConnected(ctd.ProxyDialer, ctd.connectTime)
		break
	}

	// If we've connected to more than one dialer after trying all of them,
	// switch to the next dialer that's optimized for multiple connections.
	nextOpts := fcd.opts.Clone()
	nextOpts.Dialers = fcd.connected.proxyDialers()
	fcd.next(nextOpts, fcd)
}

func (fcd *fastConnectDialer) parallelDial(dialers []ProxyDialer, connectedCh chan connectTimeProxyDialer) {
	// Dial all the dialers in parallel with a 10 second timeout for each dialer.
	// We don't want to wait too long for any one dialer to connect.
	// Do not use a wait group here because we want to return as soon as the first
	// dialer connects.
	for _, pd := range dialers {
		go func(dialer ProxyDialer) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			start := time.Now()
			conn, err := dialer.DialProxy(ctx)
			defer func() {
				if conn != nil {
					conn.Close()
				}
			}()
			if err != nil {
				log.Debugf("Dialer %v failed in %v with: %v", dialer.Name(), time.Since(start), err)
				return
			}
			connectedCh <- connectTimeProxyDialer{
				ProxyDialer: dialer,
				connectTime: time.Since(start),
			}
		}(pd)
	}
}

// Accessor for a copy of the ProxyDialer slice
func (cd *connectedDialers) proxyDialers() []ProxyDialer {
	cd.RLock()
	defer cd.RUnlock()

	dialers := make([]ProxyDialer, len(cd.dialers))

	// Note that we manually copy here vs using copy because we need an array of
	// ProxyDialers, not a dialersByConnectTime.
	for i, ctd := range cd.dialers {
		dialers[i] = ctd.ProxyDialer
	}
	return dialers
}

// onConnected adds a connected dialer to the list of connected dialers and returns
// the fastest dialer.
func (cd *connectedDialers) onConnected(pd ProxyDialer, connectTime time.Duration) ProxyDialer {
	cd.Lock()
	defer cd.Unlock()

	cd.dialers = append(cd.dialers, connectTimeProxyDialer{
		ProxyDialer: pd,
		connectTime: connectTime,
	})
	sort.Slice(cd.dialers, func(i, j int) bool {
		return cd.dialers[i].connectTime < cd.dialers[j].connectTime
	})
	return cd.dialers[0].ProxyDialer
}

// protectedDialer protects a dialer.Dialer with a RWMutex. We can't use an atomic.Value here
// because ProxyDialer is an interface.
type protectedDialer struct {
	sync.RWMutex
	dialer ProxyDialer
}

// set sets the dialer in the protectedDialer
func (pd *protectedDialer) set(dialer ProxyDialer) {
	pd.Lock()
	defer pd.Unlock()
	pd.dialer = dialer
}

// get gets the dialer from the protectedDialer
func (pd *protectedDialer) get() ProxyDialer {
	pd.RLock()
	defer pd.RUnlock()
	return pd.dialer
}
