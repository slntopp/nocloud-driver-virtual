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
	"time"

	pb "github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	ipb "github.com/slntopp/nocloud-proto/instances"
	sppb "github.com/slntopp/nocloud-proto/services_providers"
	stpb "github.com/slntopp/nocloud-proto/states"
	sttspb "github.com/slntopp/nocloud-proto/statuses"

	i "github.com/slntopp/nocloud/pkg/instances"
	"github.com/slntopp/nocloud/pkg/states"

	"github.com/slntopp/nocloud-driver-virtual/internal/pubsub"

	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type VirtualDriver struct {
	pb.UnimplementedDriverServiceServer

	log *zap.Logger

	Type string

	HandlePublishRecords       pubsub.RecordsPublisher
	HandlePublishEvent         pubsub.EventPublisher
	HandlePublishSPState       states.Pub
	HandlePublishInstanceState states.Pub
	HandlePublishInstanceData  i.Pub
}

func NewVirtualDriver(log *zap.Logger, rbmq *amqp091.Connection, _type string) *VirtualDriver {

	return &VirtualDriver{
		log: log.Named("VirtualDriver").Named(_type), Type: _type,

		HandlePublishRecords:       pubsub.SetupRecordsPublisher(log, rbmq),
		HandlePublishSPState:       pubsub.SetupSPStatesPublisher(log, rbmq),
		HandlePublishInstanceState: pubsub.SetupInstancesStatesPublisher(log, rbmq),
		HandlePublishInstanceData:  pubsub.SetupInstancesDataPublisher(log, rbmq),
		HandlePublishEvent:         pubsub.SetupEventsPublisher(log, rbmq),
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

	secrets := sp.GetSecrets()

	if secrets != nil {
		autoActivation := secrets["auto_activation"].GetBoolValue()
		if autoActivation {
			for _, inst := range igroup.GetInstances() {
				inst.State = &stpb.State{
					State: stpb.NoCloudState_RUNNING,
				}
				go s.HandlePublishInstanceState(&stpb.ObjectState{
					Uuid:  inst.GetUuid(),
					State: inst.GetState(),
				})
			}
		}
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

	for _, group := range req.GetGroups() {
		log.Debug("Monitoring Group", zap.String("uuid", group.GetUuid()), zap.String("title", group.GetTitle()), zap.Int("instances", len(group.GetInstances())))
		for _, i := range group.GetInstances() {
			log.Debug("Monitoring Instance", zap.String("uuid", i.GetUuid()), zap.String("title", i.GetTitle()))

			if i.GetData() == nil {
				i.Data = make(map[string]*structpb.Value)
			}
			instConfig := i.GetConfig()

			instConfig := i.GetConfig()

			stateNil := i.GetState() == nil
			statePending := true
			if i.GetState() != nil {
				statePending = i.GetState().GetState() == stpb.NoCloudState_PENDING
			}

			if stateNil || statePending {
				bpMeta := i.GetBillingPlan().GetMeta()

				cfgAutoStart := instConfig["auto_start"].GetBoolValue()
				autoStart := bpMeta["auto_start"].GetBoolValue()

				if autoStart || cfgAutoStart {
					i.State = &stpb.State{
						State: stpb.NoCloudState_RUNNING,
					}
					i.Data["start"] = structpb.NewNumberValue(float64(time.Now().Unix()))
					s.HandlePublishInstanceData(&ipb.ObjectData{
						Uuid: i.GetUuid(),
						Data: i.GetData(),
					})
				} else {
					i.State = &stpb.State{
						State: stpb.NoCloudState_PENDING,
					}
				}

				go s.HandlePublishInstanceState(&stpb.ObjectState{
					Uuid:  i.GetUuid(),
					State: i.GetState(),
				})
			}

			if i.GetStatus() == sttspb.NoCloudStatus_DEL {
				if i.GetState().GetState() != stpb.NoCloudState_DELETED {
					i.State.State = stpb.NoCloudState_DELETED
					s.HandlePublishInstanceState(&stpb.ObjectState{
						Uuid:  i.GetUuid(),
						State: i.GetState(),
					})
				}
				continue
			}

			_, ok := i.GetData()["creation"]

			if !ok {
				i.Data["creation"] = structpb.NewNumberValue(float64(time.Now().Unix()))
				s.HandlePublishInstanceData(&ipb.ObjectData{
					Uuid: i.GetUuid(), Data: i.GetData(),
				})
			}

			autoRenew := false

			if instConfig != nil {
				autoRenewVal, ok := instConfig["auto_renew"]
				if ok {
					autoRenew = autoRenewVal.GetBoolValue()
				}

			}

			log.Debug("Cfg", zap.String("uuid", i.GetUuid()), zap.Any("cfg", instConfig))

			balance := req.GetBalance()[group.GetUuid()]
			if autoRenew {
				go s._handleInstanceBilling(i, &balance)
			} else {
				go s._handleNonRegularBilling(i)
			}
			req.Balance[group.GetUuid()] = balance
		}
	}

	// Placeholder
	s.HandlePublishSPState(&stpb.ObjectState{
		Uuid: sp.GetUuid(),
		State: &stpb.State{
			State: stpb.NoCloudState_RUNNING,
			Meta: map[string]*structpb.Value{
				"ts": structpb.NewNumberValue(float64(time.Now().Unix())),
			},
		},
	})

	log.Info("Routine Done", zap.String("sp", sp.GetUuid()))
	return &pb.MonitoringResponse{}, nil
}
