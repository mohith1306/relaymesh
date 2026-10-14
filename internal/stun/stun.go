// Package stun implements a minimal STUN (RFC 5389) client used to
// discover a node's server-reflexive (public) address — the srflx
// candidate that remote peers behind other NATs need in order to hole
// punch to us. Only Binding requests over UDP are supported; that is
// all RelayMesh needs.
//
// No third-party dependency: the message is small and fixed enough to
// encode by hand, which keeps the go.mod dependency surface unchanged.
package stun

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	magicCookie = 0x2112A442
	bindingReq  = 0x0001
	bindingResp = 0x0101
	attrMapped  = 0x0001 // XOR-MAPPED-ADDRESS
)

var (
	ErrTimeout    = errors.New("stun: no response")
	ErrBadMessage = errors.New("stun: malformed message")
	ErrNotXOR     = errors.New("stun: response missing XOR-MAPPED-ADDRESS")
)

// Addr is a discovered public (server-reflexive) UDP address.
type Addr struct {
	IP   net.IP
	Port uint16
}

func (a Addr) String() string {
	return net.JoinHostPort(a.IP.String(), fmt.Sprintf("%d", a.Port))
}

// PublicAddr queries server (host:port, e.g. "stun.l.google.com:19302")
// from localAddr ("" = OS-chosen). It returns the address the server
// observed us sending from — our server-reflexive candidate.
func PublicAddr(server, localAddr string, timeout time.Duration) (Addr, error) {
	raddr, err := net.ResolveUDPAddr("udp4", server)
	if err != nil {
		return Addr{}, fmt.Errorf("resolve %s: %w", server, err)
	}
	var laddr *net.UDPAddr
	if localAddr != "" {
		if laddr, err = net.ResolveUDPAddr("udp4", localAddr); err != nil {
			return Addr{}, fmt.Errorf("resolve local %s: %w", localAddr, err)
		}
	}
	conn, err := net.DialUDP("udp4", laddr, raddr)
	if err != nil {
		return Addr{}, fmt.Errorf("dial %s: %w", server, err)
	}
	defer conn.Close()

	// transaction id: 12 random bytes, distinct from the magic cookie.
	var txID [12]byte
	if _, err := rand.Read(txID[:]); err != nil {
		return Addr{}, err
	}

	msg := make([]byte, 20)
	binary.BigEndian.PutUint16(msg[0:2], bindingReq)
	binary.BigEndian.PutUint16(msg[2:4], 0) // no attributes in the request
	binary.BigEndian.PutUint32(msg[4:8], magicCookie)
	copy(msg[8:20], txID[:])

	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(msg); err != nil {
		return Addr{}, fmt.Errorf("send binding: %w", err)
	}

	buf := make([]byte, 1500)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			// A UDP "connection refused" (ICMP port unreachable) or a
			// genuine deadline both mean we never got our answer.
			return Addr{}, fmt.Errorf("%w: %v", ErrTimeout, err)
		}
		if n < 20 {
			continue
		}
		msgLen := int(binary.BigEndian.Uint16(buf[2:4]))
		if msgLen < 20 || msgLen > n {
			continue
		}
		if binary.BigEndian.Uint16(buf[0:2]) != bindingResp {
			continue
		}
		if binary.BigEndian.Uint32(buf[4:8]) != magicCookie {
			continue
		}
		if string(buf[8:20]) != string(txID[:]) {
			// Not our transaction; keep waiting until the deadline.
			continue
		}
		return parseXORMapped(buf[:msgLen])
	}
}

func parseXORMapped(msg []byte) (Addr, error) {
	// Attributes start at byte 20; each is type(2) len(2) value padded to 4.
	for off := 20; off+4 <= len(msg); {
		atype := binary.BigEndian.Uint16(msg[off : off+2])
		alen := int(binary.BigEndian.Uint16(msg[off+2 : off+4]))
		off += 4
		if off+alen > len(msg) {
			break
		}
		val := msg[off : off+alen]
		if atype == attrMapped && alen >= 8 {
			// XOR-MAPPED-ADDRESS: family(1) + pad(1) + xport(2) + xaddr(4 for IPv4)
			family := val[1]
			if family != 0x01 { // IPv4 only
				return Addr{}, ErrNotXOR
			}
			xport := binary.BigEndian.Uint16(val[2:4]) ^ uint16(magicCookie>>16)
			ip := make(net.IP, 4)
			copy(ip, val[4:8])
			cookie := uint32(magicCookie)
			ip[0] ^= byte(cookie >> 24)
			ip[1] ^= byte(cookie >> 16)
			ip[2] ^= byte(cookie >> 8)
			ip[3] ^= byte(cookie)
			return Addr{IP: ip, Port: xport}, nil
		}
		// Attributes are padded to a 4-byte boundary.
		off += (alen + 3) &^ 3
	}
	return Addr{}, ErrNotXOR
}
