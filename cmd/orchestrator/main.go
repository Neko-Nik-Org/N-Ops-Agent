package main

import (
	"io"
	"log/slog"
	"net"
	"os"

	agentv1 "github.com/Neko-Nik-Org/N-Ops-Agent/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type orchestrator struct {
	agentv1.UnimplementedAgentServiceServer
}

func (o *orchestrator) Connect(stream agentv1.AgentService_ConnectServer) error {

	var serverID string

	for {

		req, err := stream.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		switch m := req.GetMsg().(type) {

		case *agentv1.ConnectRequest_Hello:
			serverID = m.Hello.GetServerId()
			slog.Info("agent hello",
				"server_id", serverID,
				"hostname", m.Hello.GetHostname(),
				"agent_version", m.Hello.GetAgentVersion(),
				"os", m.Hello.GetOs(),
				"arch", m.Hello.GetArch(),
			)

			err = stream.Send(&agentv1.ConnectResponse{Msg: &agentv1.ConnectResponse_Welcome{Welcome: &agentv1.Welcome{HeartbeatIntervalSeconds: 5}}})
			if err != nil {
				return err
			}
		case *agentv1.ConnectRequest_Heartbeat:

			if serverID == "" {
				return status.Error(codes.NotFound, "Server id is required")
			}

			slog.Info("heartbeat received",
				"server_id", serverID,
				"cpu_percent", m.Heartbeat.GetCpuPercent(),
				"memory_percent", m.Heartbeat.GetMemoryPercent(),
				"uptime_seconds", m.Heartbeat.GetUptimeSeconds(),
				"sent_at", m.Heartbeat.GetSentAt().AsTime(),
			)

			err = stream.Send(&agentv1.ConnectResponse{Msg: &agentv1.ConnectResponse_HeartbeatAck{HeartbeatAck: &agentv1.HeartbeatAck{ReceivedAt: timestamppb.Now()}}})

			if err != nil {
				return err
			}

		default:
			return status.Error(codes.InvalidArgument, "unknown message")
		}

	}

}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("faield to listen", "err", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()

	agentv1.RegisterAgentServiceServer(grpcServer, &orchestrator{})

	slog.Info("orchestrator listening", "addr", ":50051")
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}

}
