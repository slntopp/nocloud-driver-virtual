package actions

import (
	ipb "github.com/slntopp/nocloud-proto/instances"
	stpb "github.com/slntopp/nocloud-proto/states"

	"github.com/slntopp/nocloud/pkg/states"

	"google.golang.org/protobuf/types/known/structpb"
)

type ServiceAction func(states.Pub, *ipb.Instance, map[string]*structpb.Value) (*ipb.InvokeResponse, error)

var SrvActions = map[string]ServiceAction{
	"change_status": ChangeState,
}

func ChangeState(pub states.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	state := int32(data["state"].GetNumberValue())
	statepb := stpb.NoCloudState(state)

	inst.State.State = statepb

	go pub(&stpb.ObjectState{
		Uuid:  inst.GetUuid(),
		State: inst.GetState(),
	})

	return &ipb.InvokeResponse{
		Result: true,
	}, nil
}
