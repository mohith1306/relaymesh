package flow

import (
	"context"
	"net"

	"github.com/relaymesh/relaymesh/internal/node"
)

// GatewayDialer binds a Manager to a fixed gateway and exposes a plain
// TCP Dial. It satisfies proxy.Dialer, letting local applications (a
// browser, curl, WhatsApp's HTTP proxy) ride the mesh without knowing
// anything about mesh routing.
type GatewayDialer struct {
	Mgr *Manager
	Gw  node.NodeID
}

func (d *GatewayDialer) Dial(ctx context.Context, host string, port uint16) (net.Conn, error) {
	return d.Mgr.OpenTCP(ctx, d.Gw, host, port)
}
