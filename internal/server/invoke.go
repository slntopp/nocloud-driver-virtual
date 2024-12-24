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
	method := req.GetMethod()
	instance := req.GetInstance()
	sp := req.GetServicesProvider()

	log := s.log.With(zap.String("instance", instance.GetUuid()), zap.String("method", method))
	log.Debug("Invoke request received", zap.Any("action", req.Method))

	if instance.GetAccess().GetLevel() < accesspb.Level_ROOT {
		return nil, status.Errorf(codes.PermissionDenied, "Action %s is admin action", method)
	}

	action, ok := actions.SrvActions[method]
	if ok {
		return action(log, s.HandlePublishInstanceState, s.HandlePublishInstanceData, instance, req.GetParams())
	}

	action, ok = actions.BillingActions[method]
	if ok {
		if method == "manual_renew" {
			err := s._handleRenewBilling(instance)
			if err != nil {
				return &ipb.InvokeResponse{Result: false}, err
			}
		} else {
			return action(log, s.HandlePublishInstanceState, s.HandlePublishInstanceData, instance, req.GetParams())
		}
		return &ipb.InvokeResponse{Result: true}, nil
	}

	ansibleAction, ok := actions.AnsibleActions[method]
	if ok {
		secrets := sp.GetSecrets()
		ansibleSecret, ok := secrets["ansible"]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "No ansible config")
		}
		ansibleSecretValue := ansibleSecret.GetStructValue().AsMap()
		return ansibleAction(log, s.ansibleCtx, s.ansibleClient, ansibleSecretValue, instance, req.GetParams())
	}

	return nil, status.Errorf(codes.PermissionDenied, "Action %s is admin action", method)
}
