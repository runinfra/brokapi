// Package brokapi connects a Go bot to a brokapi gateway.
//
// The gateway is the program brokapi: one process per broker account, bots talk to it over gRPC on this machine. The
// generated client of its .proto files is the package gatewayv1 (github.com/runinfra/brokapi/go/gateway/v1). This
// package adds what the .proto files cannot say: where the gateway listens, the access key, the two headers of every call,
// waiting while the gateway starts, and the error of the library that a failed call carries.
//
//	conn, err := brokapi.Connect(ctx, "brokapi.state/gateways/bcs-main", "my-bot")
//	if err != nil { ... }
//	defer conn.Close()
//	health, err := conn.Gateway.Health(ctx, &gatewayv1.HealthRequest{})
//
// The book: https://brokapi.ru/docs/gateway/bots.html
package brokapi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	gatewayv1 "github.com/runinfra/brokapi/go/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Conn is a connection to one gateway. Gateway is the generated client: every call carries the key and the name of the
// bot by itself, and a failed call returns an *Error when the gateway sent the error of the library.
type Conn struct {
	Gateway gatewayv1.GatewayClient
	// Dir is the directory of the gateway, Bot the name of the bot.
	Dir, Bot string
	cc       *grpc.ClientConn
}

// Close closes the connection of this bot. The gateway goes on, the orders stay at the broker.
func (c *Conn) Close() error { return c.cc.Close() }

// GatewayDir is the directory of the gateway name of a service whose state directory is state: "<state>/gateways/<name>".
// Without state_dir in brokapi.toml the state directory is brokapi.state next to it.
func GatewayDir(state, name string) string { return filepath.Join(state, "gateways", name) }

var botName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// Options of ConnectWith. The zero value is the default.
type Options struct {
	// How long to wait for the gateway: for its address file while it is restarted, and for Health READY while it
	// reconciles its journal. 30 seconds when zero.
	ReadyTimeout time.Duration
}

// Connect connects to the gateway whose directory is dir (it holds gateway.addr and access.key) as the bot named bot, and
// waits until the gateway is ready.
//
// Connect again after the Events stream ends: the gateway may be a new process, on Windows on another port, and the
// address is read again, including changes while waiting for the gateway to become ready.
func Connect(ctx context.Context, dir, bot string) (*Conn, error) {
	return ConnectWith(ctx, dir, bot, Options{})
}

// ConnectWith is Connect with options.
func ConnectWith(ctx context.Context, dir, bot string, opts Options) (*Conn, error) {
	if !botName.MatchString(bot) {
		return nil, fmt.Errorf("brokapi: the name of the bot %q: 1 to 64 letters, digits, '.', '_', '-'", bot)
	}
	wait := opts.ReadyTimeout
	if wait == 0 {
		wait = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	key, err := os.ReadFile(filepath.Join(dir, "access.key"))
	if err != nil {
		return nil, fmt.Errorf("brokapi: no access key in %s (is it the directory of a gateway? brokapi ps): %w", dir, err)
	}
	headers := metadata.Pairs("authorization", "Bearer "+strings.TrimSpace(string(key)), "x-brokapi-bot", bot)
	var addr string
	var conn *Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		// A restart can replace the address after we already read it (a new TCP port on Windows).
		// Re-read until READY, closing the old channel when the endpoint changes.
		b, err := os.ReadFile(filepath.Join(dir, "gateway.addr"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("brokapi: %w", err)
		}
		next := strings.TrimSpace(string(b))
		if next == "" {
			if conn != nil {
				conn.Close()
				conn = nil
			}
			if pause(ctx, 100*time.Millisecond) != nil {
				return nil, fmt.Errorf("brokapi: no gateway address in %s: the gateway is not running (brokapi ps)", dir)
			}
			continue
		}
		if conn == nil || next != addr {
			if conn != nil {
				conn.Close()
				conn = nil
			}
			addr = next
			cc, err := grpc.NewClient(target(addr),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, o ...grpc.CallOption) error {
					var trailer metadata.MD
					err := invoker(metadata.NewOutgoingContext(ctx, headers), method, req, reply, cc, append(o, grpc.Trailer(&trailer))...)
					return wrap(err, trailer)
				}),
				grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, o ...grpc.CallOption) (grpc.ClientStream, error) {
					s, err := streamer(metadata.NewOutgoingContext(ctx, headers), desc, cc, method, o...)
					if err != nil {
						return nil, err
					}
					return stream{s}, nil
				}),
			)
			if err != nil {
				return nil, fmt.Errorf("brokapi: the address %q: %w", addr, err)
			}
			conn = &Conn{Gateway: gatewayv1.NewGatewayClient(cc), Dir: dir, Bot: bot, cc: cc}
		}
		h, err := conn.Gateway.Health(ctx, &gatewayv1.HealthRequest{})
		if err == nil && h.State == gatewayv1.GatewayState_GATEWAY_STATE_READY {
			ready := conn
			conn = nil // ownership passes to the caller
			return ready, nil
		}
		if status.Code(err) == codes.Unauthenticated {
			return nil, fmt.Errorf("brokapi: the gateway refused the access key of %s (the access.key of another gateway?): %w", dir, err)
		}
		if pause(ctx, 100*time.Millisecond) != nil {
			why := "still starting"
			if err != nil {
				why = err.Error()
			} else if h.Detail != "" {
				why = "still starting: " + h.Detail
			}
			return nil, fmt.Errorf("brokapi: the gateway in %s is not ready: %s", dir, why)
		}
	}
}

// target turns the content of gateway.addr into a gRPC target: "unix:/path" is one already, "tcp:127.0.0.1:port" (Windows)
// becomes "127.0.0.1:port".
func target(addr string) string {
	if rest, ok := strings.CutPrefix(addr, "tcp:"); ok {
		return "passthrough:///" + rest
	}
	return addr
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// Error is a failed call that carries the error of the library (the brokapi-error-bin trailer). Detail.Kind decides what
// to do, not the gRPC code: the table is in the book, gateway/bots.html, "Ошибки". status.FromError and status.Code work
// on it as on the error of grpc.
type Error struct {
	Status *status.Status
	Detail *gatewayv1.ErrorDetail
}

func (e *Error) Error() string {
	d := e.Detail
	text := strings.TrimPrefix(d.Kind.String(), "KIND_")
	for _, part := range []string{d.Code, d.Message, d.Capability} {
		if part != "" {
			text += ": " + part
		}
	}
	return "brokapi: " + text
}

// GRPCStatus lets status.FromError and status.Code read the gRPC status.
func (e *Error) GRPCStatus() *status.Status { return e.Status }

// Detail is the error of the library that err carries, or nil: no error, or a failure before the library (the
// connection, the key, the name of the bot).
func Detail(err error) *gatewayv1.ErrorDetail {
	var e *Error
	if errors.As(err, &e) {
		return e.Detail
	}
	return nil
}

func wrap(err error, trailer metadata.MD) error {
	if err == nil {
		return nil
	}
	v := trailer.Get("brokapi-error-bin")
	if len(v) == 0 {
		return err
	}
	d := &gatewayv1.ErrorDetail{}
	if proto.Unmarshal([]byte(v[0]), d) != nil {
		return err
	}
	return &Error{Status: status.Convert(err), Detail: d}
}

// stream reads the trailer of a stream that ends with an error, as the unary calls do.
type stream struct{ grpc.ClientStream }

func (s stream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		return wrap(err, s.ClientStream.Trailer())
	}
	return err
}
