/*
Copyright © 2023 Nikita Ivanovski info@slnt-opp.xyz

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"context"

	billingpb "github.com/slntopp/nocloud-proto/billing"
	pb "github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	sppb "github.com/slntopp/nocloud-proto/services_providers"
	"go.uber.org/zap"
)

type RecordsPublisher func([]*billingpb.Record)

type VirtualDriver struct {
	pb.UnimplementedDriverServiceServer

	log *zap.Logger

	Type                 string
	HandlePublishRecords RecordsPublisher
}

func NewVirtualDriver(log *zap.Logger, _type string) *VirtualDriver {
	return &VirtualDriver{
		log: log.Named("VirtualDriver").Named(_type), Type: _type,
	}
}

func (s *VirtualDriver) GetType(ctx context.Context, req *pb.GetTypeRequest) (*pb.GetTypeResponse, error) {
	return &pb.GetTypeResponse{Type: s.Type}, nil
}

func (s *VirtualDriver) TestServiceProviderConfig(ctx context.Context, req *pb.TestServiceProviderConfigRequest) (*sppb.TestResponse, error) {
	log := s.log.Named("TestServiceProviderConfig")
	sp := req.GetServicesProvider()
	log.Debug("Request received", zap.Any("sp", sp), zap.Bool("syntax_only", req.GetSyntaxOnly()))

	return &sppb.TestResponse{Result: true}, nil
}
