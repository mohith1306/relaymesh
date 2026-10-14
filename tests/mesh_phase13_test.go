package test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/flow"
	"github.com/relaymesh/relaymesh/internal/gateway"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/proxy"
)

// TestMeshPhase13ProxyServesLocalApps proves a laptop-class client can
// reach "the internet" through the mesh using the proxy server, with
// no knowledge of mesh internals:
//
//	app -> SOCKS5/HTTP CONNECT -> node-A proxy -> flow tunnel -> node-B -> node-GW -> origin
//
// node-A never resolves or dials the origin itself; the gateway does.
// Both SOCKS5 CONNECT (what curl --socks5 uses) and HTTP CONNECT (what
// browsers and WhatsApp's proxy setting use) are exercised.
func TestMeshPhase13ProxyServesLocalApps(t *testing.T) {
	logger := slog.Default()

	// Fake internet origin.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "origin-ok path=%s host=%s", r.URL.Path, r.Host)
	}))
	defer origin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ports := freeUDPPorts(t, 6)
	discA, discB, discG := ports[0], ports[1], ports[2]
	dataA, dataB, dataG := ports[3], ports[4], ports[5]

	newNode := func(id string, discPort, dataPort uint16, known []uint16) *mesh.MeshNode {
		return mesh.New(mesh.Config{
			NodeID:              node.NodeID(id),
			Address:             "127.0.0.1",
			DiscoveryPort:       discPort,
			DataPort:            dataPort,
			KnownDiscoveryPorts: known,
			HeartbeatInterval:   100 * time.Millisecond,
			PeerTimeout:         2 * time.Second,
		}, logger)
	}

	nodeA := newNode("node-A", discA, dataA, []uint16{discB})
	nodeB := newNode("node-B", discB, dataB, []uint16{discA, discG})
	gw := gateway.New(mesh.Config{
		NodeID:              "node-GW",
		Address:             "127.0.0.1",
		DiscoveryPort:       discG,
		DataPort:            dataG,
		KnownDiscoveryPorts: []uint16{discB},
		HeartbeatInterval:   100 * time.Millisecond,
		PeerTimeout:         2 * time.Second,
	}, logger.With("component", "gateway"))

	for _, n := range []*mesh.MeshNode{nodeA, nodeB} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}
	if err := gw.Start(ctx); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	defer gw.Stop()

	// Drain B's delivered channel; it only relays here.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-nodeB.Delivered():
			}
		}
	}()

	manager := flow.NewManager("node-A", nodeA.Send, logger.With("component", "flow"))
	nodeA.AddPayloadHandler(manager.HandlePacket)

	dialer := &flow.GatewayDialer{Mgr: manager, Gw: "node-GW"}

	waitFor(t, 10*time.Second, "A to see B and B to see the gateway", func() bool {
		return hasPeer(nodeA, "node-B") && hasPeer(nodeB, "node-GW")
	})
	waitFor(t, 10*time.Second, "A to have a route to the gateway", func() bool {
		_, ok := nodeA.Router().GetNextHop("node-GW")
		return ok
	})

	// Proxy on a local loopback port, dialing through the mesh.
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	proxyAddr := proxyLn.Addr().String()
	proxyLn.Close()
	srv := proxy.NewServer(proxyAddr, dialer, logger.With("component", "proxy"))
	go func() {
		if err := srv.ListenAndServe(ctx); err != nil {
			logger.Debug("proxy server stopped", "error", err)
		}
	}()
	defer srv.Close()

	waitFor(t, 5*time.Second, "proxy listener to accept", func() bool {
		c, cerr := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if cerr != nil {
			return false
		}
		c.Close()
		return true
	})

	originHost, _, err := net.SplitHostPort(origin.Listener.Addr().String())
	if err != nil {
		t.Fatalf("parse origin addr: %v", err)
	}

	t.Run("HTTPClientOverMesh", func(t *testing.T) {
		// In-process client that dials straight into the mesh.
		client := proxy.HTTPClient(dialer)
		rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
		defer rcancel()
		req, _ := http.NewRequestWithContext(rctx, "GET", origin.URL+"/via-mesh", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("http over mesh: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if !strings.Contains(string(body), "origin-ok path=/via-mesh") {
			t.Fatalf("body = %q, want to contain %q", body, "origin-ok path=/via-mesh")
		}
	})

	t.Run("HTTPCONNECTOverListener", func(t *testing.T) {
		// Real browser/WhatsApp path: HTTP CONNECT to the proxy port.
		// The origin is TLS so the client MUST tunnel with CONNECT,
		// which is exactly the path browsers and WhatsApp use.
		tlsOrigin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprintf(w, "origin-ok path=%s", r.URL.Path)
		}))
		defer tlsOrigin.Close()

		hc := &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return dialSOCKS5(ctx, proxyAddr, addr)
				},
				TLSClientConfig: tlsOrigin.Client().Transport.(*http.Transport).TLSClientConfig,
			},
			Timeout: 20 * time.Second,
		}
		resp, err := hc.Get(tlsOrigin.URL + "/connect")
		if err != nil {
			t.Fatalf("http connect: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if want := "origin-ok path=/connect"; string(body) != want {
			t.Fatalf("body = %q, want %q", body, want)
		}
	})

	t.Run("SOCKS5OverListener", func(t *testing.T) {
		// Raw SOCKS5 CONNECT by hand, exactly what curl --socks5-hostname does.
		conn, err := dialSOCKS5(ctx, proxyAddr, origin.Listener.Addr().String())
		if err != nil {
			t.Fatalf("socks5 dial: %v", err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

		// Plain HTTP through the established tunnel.
		_, _ = fmt.Fprintf(conn, "GET /socks HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", originHost)
		respBytes, err := io.ReadAll(conn)
		if err != nil {
			t.Fatalf("read http: %v", err)
		}
		if want := "origin-ok path=/socks"; !strings.Contains(string(respBytes), want) {
			t.Fatalf("response missing %q:\n%s", want, respBytes)
		}
	})
}

// dialSOCKS5 performs a full SOCKS5 no-auth CONNECT to target through
// the proxy listener, resolving target's host at the far end (ATYP=3).
func dialSOCKS5(ctx context.Context, proxyAddr, target string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}
	fail := func(err error) (net.Conn, error) { conn.Close(); return nil, err }
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	// Greeting: VER=5, NMETHODS=1, METHOD=0 (no auth).
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return fail(fmt.Errorf("greeting: %w", err))
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fail(fmt.Errorf("greeting reply: %w", err))
	}
	if reply[0] != 0x05 || reply[1] != 0x00 {
		return fail(fmt.Errorf("greeting reply = %v, want 05 00", reply))
	}

	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return fail(fmt.Errorf("parse target: %w", err))
	}
	portNum := uint16(0)
	if _, err := fmt.Sscanf(portStr, "%d", &portNum); err != nil {
		return fail(fmt.Errorf("parse port: %w", err))
	}
	hostBytes := []byte(host)
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(hostBytes))}
	req = append(req, hostBytes...)
	req = append(req, byte(portNum>>8), byte(portNum))
	if _, err := conn.Write(req); err != nil {
		return fail(fmt.Errorf("connect: %w", err))
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return fail(fmt.Errorf("connect reply: %w", err))
	}
	if head[1] != 0x00 {
		return fail(fmt.Errorf("socks rep = %d, want 0", head[1]))
	}
	// Consume bound address (ATYP in head[3]).
	switch head[3] {
	case 0x01:
		io.ReadFull(conn, make([]byte, 4+2))
	case 0x03:
		l := make([]byte, 1)
		io.ReadFull(conn, l)
		io.ReadFull(conn, make([]byte, int(l[0])+2))
	case 0x04:
		io.ReadFull(conn, make([]byte, 16+2))
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}
