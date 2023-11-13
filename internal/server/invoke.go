package server

import (
	"context"

	"github.com/slntopp/nocloud-driver-virtual/internal/actions"

	accesspb "github.com/slntopp/nocloud-proto/access"
	pb "github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	ipb "github.com/slntopp/nocloud-proto/instances"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *VirtualDriver) Invoke(ctx context.Context, req *pb.InvokeRequest) (*ipb.InvokeResponse, error) {
	s.log.Debug("Invoke request received", zap.Any("action", req.Method))

	method := req.GetMethod()
	instance := req.GetInstance()

	if instance.GetAccess().GetLevel() < accesspb.Level_ROOT {
		return nil, status.Errorf(codes.PermissionDenied, "Action %s is admin action", method)
	}

	actions, ok := actions.SrvActions[method]

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Action %s not declared for %s", method, s.Type)
	}

	return actions(s.HandlePublishInstanceState, s.HandlePublishInstanceData, instance, req.GetParams())
}
