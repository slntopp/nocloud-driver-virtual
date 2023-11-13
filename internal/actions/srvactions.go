package actions

import (
	ipb "github.com/slntopp/nocloud-proto/instances"
	stpb "github.com/slntopp/nocloud-proto/states"
	"time"

	"github.com/slntopp/nocloud/pkg/instances"
	"github.com/slntopp/nocloud/pkg/states"

	"google.golang.org/protobuf/types/known/structpb"
)

type ServiceAction func(states.Pub, instances.Pub, *ipb.Instance, map[string]*structpb.Value) (*ipb.InvokeResponse, error)

var SrvActions = map[string]ServiceAction{
	"change_state": ChangeState,
}

func ChangeState(sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	state := int32(data["state"].GetNumberValue())
	statepb := stpb.NoCloudState(state)

	if inst.State == nil {
		inst.State = &stpb.State{}
	}

	inst.State.State = statepb

	_, err := sPub(&stpb.ObjectState{
		Uuid:  inst.GetUuid(),
		State: inst.GetState(),
	})

	iData := inst.GetData()

	_, ok := iData["start"]

	if statepb == stpb.NoCloudState_RUNNING && !ok {
		iData["start"] = structpb.NewStringValue(time.Now().Format("2006-01-02"))
		iPub(&ipb.ObjectData{
			Uuid: inst.GetUuid(),
			Data: iData,
		})
	}

	if err != nil {
		return nil, err
	}

	return &ipb.InvokeResponse{
		Result: true,
	}, nil
}
