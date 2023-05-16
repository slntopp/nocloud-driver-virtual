package server

import (
	"context"
	pb "github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	ipb "github.com/slntopp/nocloud-proto/instances"
)

func (s *VirtualDriver) Invoke(ctx context.Context, req *pb.InvokeRequest) (*ipb.InvokeResponse, error) {
	return nil, nil
}
