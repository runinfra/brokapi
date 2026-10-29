package brokapi

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gatewayv1 "github.com/runinfra/brokapi/go/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type healthGateway struct {
	gatewayv1.UnimplementedGatewayServer
	state gatewayv1.GatewayState
	seen  chan struct{}
	once  sync.Once
}

func (s *healthGateway) Health(ctx context.Context, _ *gatewayv1.HealthRequest) (*gatewayv1.HealthResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer test-key" ||
		len(md.Get("x-brokapi-bot")) != 1 || md.Get("x-brokapi-bot")[0] != "restarting-bot" {
		return nil, status.Error(codes.Unauthenticated, "missing gateway credentials")
	}
	s.once.Do(func() { close(s.seen) })
	return &gatewayv1.HealthResponse{State: s.state}, nil
}

func serveHealth(t *testing.T, state gatewayv1.GatewayState) (string, *healthGateway, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &healthGateway{state: state, seen: make(chan struct{})}
	server := grpc.NewServer()
	gatewayv1.RegisterGatewayServer(server, s)
	served := make(chan error, 1)
	t.Cleanup(func() {
		server.Stop()
		if err := <-served; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("serve the health endpoint: %v", err)
		}
	})
	go func() {
		served <- server.Serve(l)
	}()
	return "tcp:" + l.Addr().String(), s, server.Stop
}

func followsChangedAddress(t *testing.T, stopOld bool) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "access.key"), []byte("test-key"), 0600); err != nil {
		t.Fatal(err)
	}
	oldAddr, old, stop := serveHealth(t, gatewayv1.GatewayState_GATEWAY_STATE_STARTING)
	newAddr, next, _ := serveHealth(t, gatewayv1.GatewayState_GATEWAY_STATE_READY)
	address := filepath.Join(dir, "gateway.addr")
	if err := os.WriteFile(address, []byte(oldAddr), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		conn *Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := ConnectWith(ctx, dir, "restarting-bot", Options{ReadyTimeout: 3 * time.Second})
		done <- result{conn, err}
	}()
	// The client must have read the old file and actually reached the old endpoint. No timed pause.
	select {
	case <-old.seen:
	case <-ctx.Done():
		t.Fatal("the client never reached the original address")
	}
	if stopOld {
		stop()
	}
	if err := os.WriteFile(address, []byte(newAddr), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("connect to the replacement gateway: %v", r.err)
		}
		defer r.conn.Close()
		select {
		case <-next.seen:
		default:
			t.Fatal("Connect returned without reaching the replacement gateway")
		}
	case <-ctx.Done():
		t.Fatal("Connect did not follow the updated address")
	}
}

func TestConnectFollowsAnAddressChangedWhileWaitingForReady(t *testing.T) {
	followsChangedAddress(t, false)
}

func TestConnectFollowsAnAddressAfterTheOldGatewayStops(t *testing.T) {
	followsChangedAddress(t, true)
}
