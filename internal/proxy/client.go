package proxy

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"
)

// HTTPClient builds an http.Client whose connections go through the
// mesh proxy via SOCKS5 CONNECT. This is the in-process path used by
// tests and by any Go code that wants to ride the tunnel without a
// local proxy listener.
func HTTPClient(d Dialer) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				host, portStr, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				port64, err := strconv.ParseUint(portStr, 10, 16)
				if err != nil {
					return nil, err
				}
				return d.Dial(ctx, host, uint16(port64))
			},
		},
		Timeout: 30 * time.Second,
	}
}
