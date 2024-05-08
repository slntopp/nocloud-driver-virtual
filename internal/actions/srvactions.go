package actions

import (
	"slices"
	"time"

	billingpb "github.com/slntopp/nocloud-proto/billing"
	ipb "github.com/slntopp/nocloud-proto/instances"
	stpb "github.com/slntopp/nocloud-proto/states"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/slntopp/nocloud/pkg/instances"
	"github.com/slntopp/nocloud/pkg/states"

	"google.golang.org/protobuf/types/known/structpb"
)

type ServiceAction func(states.Pub, instances.Pub, *ipb.Instance, map[string]*structpb.Value) (*ipb.InvokeResponse, error)

var SrvActions = map[string]ServiceAction{
	"change_state": ChangeState,
	"freeze":       Freeze,
	"unfreeze":     Unfreeze,
}

var BillingActions = map[string]ServiceAction{
	"manual_renew": ManualRenew,
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

func Freeze(sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	inst.Data["freeze"] = structpb.NewBoolValue(true)
	iPub(&ipb.ObjectData{
		Uuid: inst.GetUuid(),
		Data: inst.GetData(),
	})

	return &ipb.InvokeResponse{
		Result: true,
	}, nil
}

func Unfreeze(sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	inst.Data["freeze"] = structpb.NewBoolValue(false)
	iPub(&ipb.ObjectData{
		Uuid: inst.GetUuid(),
		Data: inst.GetData(),
	})

	return &ipb.InvokeResponse{
		Result: true,
	}, nil
}

func ManualRenew(sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	instData := inst.GetData()
	instProduct := inst.GetProduct()
	billingPlan := inst.GetBillingPlan()

	kind := billingPlan.GetKind()
	if kind != billingpb.PlanKind_STATIC {
		return &ipb.InvokeResponse{Result: false}, status.Error(codes.Internal, "Not implemented for dynamic plan")
	}

	lastMonitoring, ok := instData["last_monitoring"]
	if !ok {
		return &ipb.InvokeResponse{Result: false}, status.Error(codes.Internal, "No last_monitoring data")
	}
	lastMonitoringValue := int64(lastMonitoring.GetNumberValue())

	period := billingPlan.GetProducts()[instProduct].GetPeriod()

	lastMonitoringValue += period
	instData["last_monitoring"] = structpb.NewNumberValue(float64(lastMonitoringValue))

	var configAddons, productAddons []any
	config := inst.GetConfig()
	if config != nil {
		configAddons = config["addons"].GetListValue().AsSlice()
	}

	meta := billingPlan.GetProducts()[instProduct].GetMeta()
	if meta != nil {
		productAddons = meta["addons"].GetListValue().AsSlice()
	}

	for _, resource := range billingPlan.Resources {
		var key any = resource.GetKey()
		if slices.Contains(configAddons, key) && slices.Contains(productAddons, key) {
			if lm, ok := instData[resource.GetKey()+"_last_monitoring"]; ok {
				lmVal := lm.GetNumberValue()
				lmVal += float64(resource.GetPeriod())
				instData[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(lmVal)
			}
		}
	}

	iPub(&ipb.ObjectData{
		Uuid: inst.GetUuid(),
		Data: instData,
	})
	return &ipb.InvokeResponse{Result: true}, nil
}
