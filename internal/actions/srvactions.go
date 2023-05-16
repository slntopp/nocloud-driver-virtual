package actions

import (
	ipb "github.com/slntopp/nocloud-proto/instances"
	"google.golang.org/protobuf/types/known/structpb"
)

type ServiceAction func(*ipb.Instance, map[string]*structpb.Value) (*ipb.InvokeResponse, error)

var SrvActions = map[string]ServiceAction{
	"change_status": ChangeStatus,
}

func ChangeStatus(inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	return nil, nil
}
