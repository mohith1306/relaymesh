package stun

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"
)

// fakeSTUN answers a Binding request with an XOR-MAPPED-ADDRESS that
// reflects the observed source, mimicking a real STUN server.
func fakeSTUN(t *testing.T) (addr string, stop func()) {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 1500)
		for {
			n, remote, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			t.Logf("fakeSTUN got %d bytes from %v", n, remote)
			if n < 20 {
				continue
			}
			// Build a response: same type/txid, plus XOR-MAPPED-ADDRESS.
			resp := make([]byte, 32)
			binary.BigEndian.PutUint16(resp[0:2], bindingResp)
			// [2:4] is the TOTAL message length (12 header + 12 attribute = 32),
			// not the attribute length.
			binary.BigEndian.PutUint16(resp[2:4], uint16(len(resp)))
			binary.BigEndian.PutUint32(resp[4:8], magicCookie)
			copy(resp[8:20], buf[8:20]) // echo txid
			binary.BigEndian.PutUint16(resp[20:22], attrMapped)
			binary.BigEndian.PutUint16(resp[22:24], 8)
			resp[25] = 0x01 // IPv4
			ip := remote.(*net.UDPAddr).IP.To4()
			binary.BigEndian.PutUint16(resp[26:28],
				uint16(remote.(*net.UDPAddr).Port)^uint16(magicCookie>>16))
			copy(resp[28:32], ip)
			cookie := uint32(magicCookie)
			resp[28] ^= byte(cookie >> 24)
			resp[29] ^= byte(cookie >> 16)
			resp[30] ^= byte(cookie >> 8)
			resp[31] ^= byte(cookie)
			_, _ = conn.WriteTo(resp, remote)
		}
	}()
	return conn.LocalAddr().String(), func() { conn.Close(); <-done }
}

func TestPublicAddr(t *testing.T) {
	server, stop := fakeSTUN(t)
	defer stop()

	got, err := PublicAddr(server, "", 2*time.Second)
	if err != nil {
		t.Fatalf("PublicAddr: %v", err)
	}
	if got.IP.String() != "127.0.0.1" {
		t.Fatalf("IP = %s, want 127.0.0.1", got.IP)
	}
	if got.Port == 0 {
		t.Fatalf("Port = 0, want non-zero")
	}
	if got.String() == "" {
		t.Fatalf("String() empty")
	}
}

func TestPublicAddrTimeout(t *testing.T) {
	// A UDP endpoint that will never respond: the client should fail with
	// an error wrapping ErrTimeout (either a real deadline or an ICMP
	// unreachable on loopback).
	_, err := PublicAddr("127.0.0.1:1", "", 150*time.Millisecond)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
}
