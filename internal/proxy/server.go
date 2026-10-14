// Package proxy serves local applications: a single listener that
// speaks both SOCKS5 (CONNECT) and HTTP (CONNECT), dialing targets
// through a Dialer — typically flow.Manager, which tunnels TCP
// sessions through the mesh gateway. This is how a laptop browser or
// WhatsApp's HTTP proxy setting rides on RelayMesh without the phone
// or laptop being a full mesh node.
package proxy

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
)

// Dialer opens a TCP session to host:port. flow.Manager satisfies it.
type Dialer interface {
	Dial(ctx context.Context, host string, port uint16) (net.Conn, error)
}

// Server accepts SOCKS5 and HTTP CONNECT clients on one listener.
type Server struct {
	addr   string
	dialer Dialer
	logger *slog.Logger

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
}

func NewServer(addr string, dialer Dialer, logger *slog.Logger) *Server {
	return &Server{
		addr:   addr,
		dialer: dialer,
		logger: logger,
		conns:  make(map[net.Conn]struct{}),
	}
}

// ListenAndServe runs until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("proxy listen %s: %w", s.addr, err)
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	s.logger.Info("proxy listening", "addr", s.addr, "protocols", "socks5,http-connect")

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handle(ctx, conn)
	}
}

// Close stops the listener and drops active clients.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.ln != nil {
		s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.conns = make(map[net.Conn]struct{})
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	br := bufio.NewReader(conn)
	first, err := br.Peek(1)
	if err != nil {
		return
	}
	var upstream net.Conn
	if first[0] == 0x05 {
		upstream, err = s.socks5Handshake(ctx, conn, br)
	} else {
		upstream, err = s.httpConnectHandshake(ctx, conn, br)
	}
	if err != nil {
		s.logger.Debug("proxy handshake failed", "client", conn.RemoteAddr(), "error", err)
		return
	}
	defer upstream.Close()
	s.logger.Info("proxy session established", "client", conn.RemoteAddr(), "target", upstream.RemoteAddr())
	pipe(conn, upstream)
}

// pipe copies both directions and tears down when either side ends.
func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(a, b); done <- struct{}{} }()
	go func() { _, _ = io.Copy(b, a); done <- struct{}{} }()
	<-done
	a.Close()
	b.Close()
	<-done
}

// --- SOCKS5 (RFC 1928, no-auth, CONNECT only) ---

func (s *Server) socks5Handshake(ctx context.Context, conn net.Conn, br *bufio.Reader) (net.Conn, error) {
	// Greeting: VER NMETHODS METHODS...
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return nil, err
	}
	if hdr[0] != 0x05 {
		return nil, fmt.Errorf("not socks5 (ver %d)", hdr[0])
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(br, methods); err != nil {
		return nil, err
	}
	// No-auth only.
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return nil, err
	}

	// Request: VER CMD RSV ATYP DST.ADDR DST.PORT
	req := make([]byte, 4)
	if _, err := io.ReadFull(br, req); err != nil {
		return nil, err
	}
	if req[0] != 0x05 {
		return nil, fmt.Errorf("bad request version %d", req[0])
	}
	if req[1] != 0x01 { // CONNECT only
		_ = socks5Reply(conn, 0x07) // command not supported
		return nil, fmt.Errorf("unsupported socks cmd %d", req[1])
	}
	host, err := socks5ReadAddr(br, req[3])
	if err != nil {
		_ = socks5Reply(conn, 0x08)
		return nil, err
	}
	portRaw := make([]byte, 2)
	if _, err := io.ReadFull(br, portRaw); err != nil {
		return nil, err
	}
	port := binary.BigEndian.Uint16(portRaw)

	upstream, err := s.dialer.Dial(ctx, host, port)
	if err != nil {
		_ = socks5Reply(conn, 0x05) // connection refused
		return nil, fmt.Errorf("dial %s:%d: %w", host, port, err)
	}
	if err := socks5Reply(conn, 0x00); err != nil {
		upstream.Close()
		return nil, err
	}
	return upstream, nil
}

func socks5ReadAddr(br *bufio.Reader, atyp byte) (string, error) {
	switch atyp {
	case 0x01: // IPv4
		raw := make([]byte, 4)
		if _, err := io.ReadFull(br, raw); err != nil {
			return "", err
		}
		return net.IP(raw).String(), nil
	case 0x03: // domain
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(br, lenBuf); err != nil {
			return "", err
		}
		raw := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(br, raw); err != nil {
			return "", err
		}
		return string(raw), nil
	case 0x04: // IPv6
		raw := make([]byte, 16)
		if _, err := io.ReadFull(br, raw); err != nil {
			return "", err
		}
		return net.IP(raw).String(), nil
	default:
		return "", fmt.Errorf("bad atyp %d", atyp)
	}
}

func socks5Reply(conn net.Conn, rep byte) error {
	// VER REP RSV ATYP=IPv4 BND.ADDR=0.0.0.0 BND.PORT=0
	_, err := conn.Write([]byte{0x05, rep, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}

// --- HTTP CONNECT ---

func (s *Server) httpConnectHandshake(ctx context.Context, conn net.Conn, br *bufio.Reader) (net.Conn, error) {
	req, err := http.ReadRequest(br)
	if err != nil {
		return nil, err
	}
	if req.Method != http.MethodConnect {
		// Plain HTTP proxying is not supported; clients should use
		// CONNECT (browsers and WhatsApp do).
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n\r\n")
		return nil, fmt.Errorf("unsupported method %s", req.Method)
	}
	host, portStr, err := net.SplitHostPort(req.Host)
	if err != nil {
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 400 Bad Request\r\n\r\n")
		return nil, fmt.Errorf("bad CONNECT host %q", req.Host)
	}
	port64, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 400 Bad Request\r\n\r\n")
		return nil, fmt.Errorf("bad CONNECT port %q", portStr)
	}

	upstream, err := s.dialer.Dial(ctx, host, uint16(port64))
	if err != nil {
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return nil, fmt.Errorf("dial %s:%s: %w", host, portStr, err)
	}
	if _, err := conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		upstream.Close()
		return nil, err
	}
	return upstream, nil
}
