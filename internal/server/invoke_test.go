package server

import (
	"context"
	"testing"

	"github.com/slntopp/nocloud-driver-virtual/internal/actions"
	accesspb "github.com/slntopp/nocloud-proto/access"
	billingpb "github.com/slntopp/nocloud-proto/billing"
	pb "github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	ipb "github.com/slntopp/nocloud-proto/instances"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func invokeAs(level accesspb.Level, method string) error {
	s := &VirtualDriver{log: zap.NewNop()}
	_, err := s.Invoke(context.Background(), &pb.InvokeRequest{
		Method: method,
		Instance: &ipb.Instance{
			Uuid:        "inst",
			Access:      &accesspb.Access{Level: level},
			BillingPlan: &billingpb.Plan{Kind: billingpb.PlanKind_DYNAMIC},
		},
	})
	return err
}

func TestAdminActionsNeedRoot(t *testing.T) {
	for method := range map[string]bool{"free_renew": true, "cancel_renew": true, "change_state": true, "freeze": true, "unfreeze": true} {
		t.Run(method, func(t *testing.T) {
			if code := status.Code(invokeAs(accesspb.Level_ADMIN, method)); code != codes.PermissionDenied {
				t.Fatalf("owner with ADMIN: code = %v, want PermissionDenied", code)
			}
		})
	}
}

func TestRootPassesAdminGate(t *testing.T) {
	// A dynamic plan makes the billing actions stop right after the gate.
	for _, method := range []string{"free_renew", "cancel_renew"} {
		t.Run(method, func(t *testing.T) {
			err := invokeAs(accesspb.Level_ROOT, method)
			if code := status.Code(err); code == codes.PermissionDenied || err == nil {
				t.Fatalf("root: err = %v, want the action's own error", err)
			}
		})
	}
}

func TestOwnerKeepsManualRenewAccess(t *testing.T) {
	if actions.AdminActions["manual_renew"] {
		t.Fatal("manual_renew is paid and must stay available to the owner")
	}
}
