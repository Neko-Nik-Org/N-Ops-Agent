package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/Neko-Nik-Org/N-Ops-Agent/gen/agent/v1"
)

const agentVersion = "0.1.0"

func run(serverID, hostname string) error {

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {

		return fmt.Errorf("Can not connect to grpc client: %w", err)
	}
	defer conn.Close()

	client := agentv1.NewAgentServiceClient(conn)

	stream, err := client.Connect(context.Background())

	if err != nil {

		return fmt.Errorf("Can not connect to client stream : %w", err)
	}

	err = stream.Send(&agentv1.ConnectRequest{Msg: &agentv1.ConnectRequest_Hello{Hello: &agentv1.Hello{ServerId: serverID, AgentVersion: agentVersion, Hostname: hostname, Os: runtime.GOOS, Arch: runtime.GOARCH}}})

	if err != nil {

		return fmt.Errorf("Can not stream welcome: %w", err)
	}

	resp, err := stream.Recv()

	if err != nil {

		return fmt.Errorf("Can not receive stream resp: %w", err)
	}
	interval := resp.GetWelcome().GetHeartbeatIntervalSeconds()

	if interval <= 0 {

		return fmt.Errorf("interval must be positive: interval= %d", interval)
	}

	slog.Info("connected", "server_id", serverID, "interval_seconds", interval)

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		err = stream.Send(&agentv1.ConnectRequest{Msg: &agentv1.ConnectRequest_Heartbeat{Heartbeat: &agentv1.Heartbeat{SentAt: timestamppb.Now(), CpuPercent: 12.6, MemoryPercent: 28.4, UptimeSeconds: 102}}})

		if err != nil {

			return fmt.Errorf("Can not stream heartbeat: %w", err)
		}

		resp, err := stream.Recv()
		if err != nil {

			return fmt.Errorf("Can not receive stream ack: %w", err)

		}
		slog.Info("heartbeat ack", "received_at", resp.GetHeartbeatAck().GetReceivedAt().AsTime())

	}
	return nil
}

func main() {
	serverID := os.Getenv("NOPS_SERVER_ID")

	if serverID == "" {
		serverID = "dev-server-1"
	}

	hostname, err := os.Hostname()

	if err != nil {
		slog.Error("Can not resolve hostname", "err", err)
		os.Exit(1)
	}

	backoff := time.Second
	const maxBackoff = 60 * time.Second

	for {
		start := time.Now()
		err := run(serverID, hostname)

		if time.Since(start) > 30*time.Second {
			backoff = time.Second
		}

		wait := backoff + time.Duration(rand.Int64N(int64(backoff/2)))
		slog.Warn("disconnected, retrying", "err", err, "retry_in", wait)
		time.Sleep(wait)
		backoff = min(2*backoff, maxBackoff)

	}

}
