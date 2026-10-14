package ai

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"testing"
	"time"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func generateTestCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "relaymesh-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("ca parse: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("leaf key marshal: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("keypair: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return cert, pool
}

func startMockAI_TLS(t *testing.T, mock *mockAIServer) (addr string, pool *x509.CertPool, stop func()) {
	t.Helper()
	cert, p := generateTestCert(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})))
	pb.RegisterAIServiceServer(srv, mock)
	go srv.Serve(lis)
	return lis.Addr().String(), p, srv.Stop
}

// TLS client trusts the server CA and completes a real RPC over TLS.
func TestClientTLSConnect(t *testing.T) {
	mock := &mockAIServer{}
	addr, pool, stop := startMockAI_TLS(t, mock)
	defer stop()

	client := NewClient(addr, slog.Default())
	client.SetTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("TLS connect failed: %v", err)
	}
	defer client.Close()

	callCtx, callCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer callCancel()
	// Mock does not implement GetModelInfo; Unimplemented proves a full
	// TLS + HTTP/2 round trip reached the server.
	_, err := client.GetModelInfo(callCtx)
	if err == nil || status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented from server after TLS round trip, got: %v", err)
	}
}

// Insecure client must fail to talk to a TLS server.
func TestClientTLSRequired(t *testing.T) {
	mock := &mockAIServer{}
	addr, _, stop := startMockAI_TLS(t, mock)
	defer stop()

	client := NewClient(addr, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err == nil {
		client.Close()
		t.Fatalf("expected insecure connect to TLS server to fail")
	}
}

// EnableTLS marks the client as TLS (smoke check for the flag path).
func TestClientEnableTLSSmoke(t *testing.T) {
	c := NewClient("127.0.0.1:1", slog.Default())
	if c.useTLS {
		t.Fatalf("expected useTLS=false by default")
	}
	c.EnableTLS()
	if !c.useTLS {
		t.Fatalf("expected useTLS=true after EnableTLS")
	}
}
