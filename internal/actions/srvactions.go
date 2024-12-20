package actions

import (
	"fmt"
	"github.com/slntopp/nocloud-driver-virtual/internal/utils"
	"go.uber.org/zap"
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

type ServiceAction func(*zap.Logger, states.Pub, instances.Pub, *ipb.Instance, map[string]*structpb.Value) (*ipb.InvokeResponse, error)

var SrvActions = map[string]ServiceAction{
	"change_state": ChangeState,
	"freeze":       Freeze,
	"unfreeze":     Unfreeze,
	"cancel_renew": CancelRenew,
}

var BillingActions = map[string]ServiceAction{
	"manual_renew": nil,
	"cancel_renew": CancelRenew,
	"free_renew":   FreeRenew,
}

func ChangeState(log *zap.Logger, sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
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

func Freeze(log *zap.Logger, sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	inst.Data["freeze"] = structpb.NewBoolValue(true)
	iPub(&ipb.ObjectData{
		Uuid: inst.GetUuid(),
		Data: inst.GetData(),
	})

	return &ipb.InvokeResponse{
		Result: true,
	}, nil
}

func Unfreeze(log *zap.Logger, sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	inst.Data["freeze"] = structpb.NewBoolValue(false)
	iPub(&ipb.ObjectData{
		Uuid: inst.GetUuid(),
		Data: inst.GetData(),
	})

	return &ipb.InvokeResponse{
		Result: true,
	}, nil
}

func FreeRenew(log *zap.Logger, sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	log.Info("Request received")

	instData := inst.GetData()
	instProduct := inst.GetProduct()
	billingPlan := inst.GetBillingPlan()

	kind := billingPlan.GetKind()
	if kind != billingpb.PlanKind_STATIC {
		log.Info("Not implemented for dynamic plan")
		return &ipb.InvokeResponse{Result: false}, status.Error(codes.Internal, "Not implemented for dynamic plan")
	}

	lastMonitoring, ok := instData["last_monitoring"]
	if !ok {
		log.Error("No last_monitoring data")
		return &ipb.InvokeResponse{Result: false}, status.Error(codes.Internal, "No last_monitoring data")
	}
	lastMonitoringValue := int64(lastMonitoring.GetNumberValue())

	product, ok := billingPlan.GetProducts()[instProduct]
	if !ok {
		log.Error("Product not found")
		return &ipb.InvokeResponse{Result: false}, status.Error(codes.Internal, "Product not found")
	}
	period := product.GetPeriod()
	pkind := product.GetPeriodKind()

	end := lastMonitoringValue + period
	if pkind != billingpb.PeriodKind_DEFAULT {
		end = utils.AlignPaymentDate(lastMonitoringValue, end, period)
	}
	instData["last_monitoring"] = structpb.NewNumberValue(float64(end))

	for _, addonId := range inst.Addons {
		key := fmt.Sprintf("addon_%s_last_monitoring", addonId)
		lmValue, ok := instData[key]
		if ok {
			lm := int64(lmValue.GetNumberValue())
			end := lm + period
			if pkind != billingpb.PeriodKind_DEFAULT {
				end = utils.AlignPaymentDate(lm, end, period)
			}
			instData[key] = structpb.NewNumberValue(float64(end))
		}
	}

	log.Info("Publishing renewed instance data")
	iPub(&ipb.ObjectData{
		Uuid: inst.GetUuid(),
		Data: instData,
	})
	log.Info("Finished")
	return &ipb.InvokeResponse{Result: true}, nil
}

func CancelRenew(log *zap.Logger, sPub states.Pub, iPub instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
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

	lastMonitoringValue = utils.AlignPaymentDate(lastMonitoringValue, lastMonitoringValue-period, period)
	instData["last_monitoring"] = structpb.NewNumberValue(float64(lastMonitoringValue))

	for _, addonId := range inst.Addons {
		key := fmt.Sprintf("addon_%s_last_monitoring", addonId)
		lmValue, ok := instData[key]
		if ok {
			lm := int64(lmValue.GetNumberValue())
			lm = utils.AlignPaymentDate(lm, lm-period, period)
			instData[key] = structpb.NewNumberValue(float64(lm))
		}
	}

	utils.SendActualMonitoringData(instData, instData, inst.GetUuid(), iPub)
	return &ipb.InvokeResponse{Result: true}, nil
}
