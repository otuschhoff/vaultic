package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	socket := os.Getenv("VAULTICDB_SOCKET")
	if socket == "" {
		socket = "/tmp/vaulticdb/vaulticdb.sock"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(
		"passthrough:///vaulticdb",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socket)
		}),
	)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := vaulticdbv1.NewVaulticDBClient(conn)
	requestContext := &vaulticdbv1.RequestContext{
		RequestId:      "vaulticdb-smoke",
		DeadlineUnixMs: time.Now().Add(10 * time.Second).UnixMilli(),
	}
	health, err := client.Health(ctx, &vaulticdbv1.HealthRequest{Context: requestContext})
	if err != nil {
		panic(err)
	}
	capabilities, err := client.Capabilities(ctx, &vaulticdbv1.CapabilitiesRequest{Context: requestContext})
	if err != nil {
		panic(err)
	}
	if !health.GetReady() || capabilities.GetProtocolVersion() != "vaulticdb.v1" {
		panic("vaulticdb returned invalid health or capability response")
	}
	if _, err := client.Shutdown(ctx, &vaulticdbv1.Empty{Context: requestContext}); err != nil {
		panic(err)
	}
	fmt.Printf("vaulticdb smoke ok: daemon=%s protocol=%s schema=%s\n", health.GetDaemonId(), health.GetProtocolVersion(), health.GetSchemaVersion())
}
