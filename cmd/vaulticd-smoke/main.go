package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	vaulticdv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	socket := os.Getenv("VAULTICD_SOCKET")
	if socket == "" {
		socket = "/tmp/vaulticd/vaulticd.sock"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "passthrough:///vaulticd", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socket)
	}), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := vaulticdv1.NewVaulticDaemonClient(conn)
	requestContext := &vaulticdv1.RequestContext{
		RequestId:      "vaulticd-smoke",
		DeadlineUnixMs: time.Now().Add(10 * time.Second).UnixMilli(),
	}
	health, err := client.Health(ctx, &vaulticdv1.HealthRequest{Context: requestContext})
	if err != nil {
		panic(err)
	}
	capabilities, err := client.Capabilities(ctx, &vaulticdv1.CapabilitiesRequest{Context: requestContext})
	if err != nil {
		panic(err)
	}
	if !health.GetReady() || capabilities.GetProtocolVersion() != "vaulticd.v1" {
		panic("vaulticd returned invalid health or capability response")
	}
	if _, err := client.Shutdown(ctx, &vaulticdv1.Empty{Context: requestContext}); err != nil {
		panic(err)
	}
	fmt.Printf("vaulticd smoke ok: daemon=%s protocol=%s schema=%s\n", health.GetDaemonId(), health.GetProtocolVersion(), health.GetSchemaVersion())
}
