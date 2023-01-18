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

	ipb "github.com/slntopp/nocloud-proto/instances"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	billingpb "github.com/slntopp/nocloud-proto/billing"
	pb "github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	sppb "github.com/slntopp/nocloud-proto/services_providers"
	"go.uber.org/zap"
)

type RecordsPublisher func([]*billingpb.Record)

type VirtualDriver struct {
	pb.UnimplementedDriverServiceServer

	log *zap.Logger

	Type string

	HandlePublishRecords       pubsub.RecordsPublisher
	HandlePublishSPState       states.Pub
	HandlePublishInstanceState states.Pub
	HandlePublishInstanceData  instances.Pub
}

func NewVirtualDriver(log *zap.Logger, rbmq *amqp091.Connection, _type string) *VirtualDriver {

	return &VirtualDriver{
		log: log.Named("VirtualDriver").Named(_type), Type: _type,

		HandlePublishRecords:       pubsub.SetupRecordsPublisher(log, rbmq),
		HandlePublishSPState:       pubsub.SetupSPStatesPublisher(log, rbmq),
		HandlePublishInstanceState: pubsub.SetupInstancesStatesPublisher(log, rbmq),
		HandlePublishInstanceData:  pubsub.SetupInstancesDataPublisher(log, rbmq),
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

func (s *VirtualDriver) TestInstancesGroupConfig(ctx context.Context, req *ipb.TestInstancesGroupConfigRequest) (*ipb.TestInstancesGroupConfigResponse, error) {
	log := s.log.Named("TestInstancesGroupConfig")
	log.Debug("Request received", zap.Any("request", req))

	return &ipb.TestInstancesGroupConfigResponse{Result: true}, nil
}

func (s *VirtualDriver) Up(ctx context.Context, input *pb.UpRequest) (*pb.UpResponse, error) {
	log := s.log.Named("Up")
	igroup := input.GetGroup()
	sp := input.GetServicesProvider()
	log.Debug("Request received", zap.Any("instances_group", igroup), zap.String("sp", sp.GetUuid()))

	if igroup.GetType() != s.Type {
		return nil, status.Error(codes.InvalidArgument, "Wrong driver type")
	}

	s.Monitoring(ctx, &pb.MonitoringRequest{Groups: []*ipb.InstancesGroup{igroup}, ServicesProvider: sp, Scheduled: false})

	log.Debug("Up request completed", zap.Any("instances_group", igroup))
	return &pb.UpResponse{
		Group: igroup,
	}, nil
}

func (s *VirtualDriver) Down(ctx context.Context, input *pb.DownRequest) (*pb.DownResponse, error) {
	log := s.log.Named("Down")
	igroup := input.GetGroup()
	sp := input.GetServicesProvider()
	log.Debug("Request received", zap.Any("instances_group", igroup), zap.String("sp", sp.GetUuid()))

	log.Debug("Down request completed", zap.Any("instances_group", igroup))
	return &pb.DownResponse{Group: igroup}, nil
}

func (s *VirtualDriver) Monitoring(ctx context.Context, req *pb.MonitoringRequest) (*pb.MonitoringResponse, error) {
	log := s.log.Named("Monitoring")
	sp := req.GetServicesProvider()
	log.Info("Starting Routine", zap.String("sp", sp.GetUuid()))

	log.Info("Routine Done", zap.String("sp", sp.GetUuid()))
	return &pb.MonitoringResponse{}, nil
}
