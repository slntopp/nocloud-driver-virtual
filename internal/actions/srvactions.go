package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/slntopp/nocloud-driver-virtual/internal/utils"
	"github.com/slntopp/nocloud-proto/ansible"
	"github.com/slntopp/nocloud/pkg/nocloud/auth"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"path"
	"strings"
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

type AnsibleAction func(
	*zap.Logger,
	context.Context,
	ansible.AnsibleServiceClient,
	map[string]any,
	*ipb.Instance,
	map[string]*structpb.Value,
) (*ipb.InvokeResponse, error)

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

var AnsibleActions = map[string]AnsibleAction{
	"vpn": VpnAction,
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

type AnsibleError struct {
	Code        string `json:"code"`
	Message     string `json:"message"`
	UserMessage string `json:"user_message"`
}

const (
	codeUnreachable   = "UNREACHABLE"
	codeUnsupportedOS = "NOT_SUPPORTED_OS"
	codeStopped       = "STOPPED"
	codeInternal      = "INTERNAL"
)

func VpnAction(
	log *zap.Logger,
	ctx context.Context,
	client ansible.AnsibleServiceClient,
	ansibleParams map[string]any,
	inst *ipb.Instance,
	data map[string]*structpb.Value,
) (*ipb.InvokeResponse, error) {
	playbookUp, ok := ansibleParams["playbook_vpn_up"].(string)
	if !ok {
		return nil, fmt.Errorf("no up playbook in sp")
	}
	playbookStart, ok := ansibleParams["playbook_vpn_start"].(string)
	if !ok {
		return nil, fmt.Errorf("no start playbook in sp")
	}
	playbookDown, ok := ansibleParams["playbook_vpn_down"].(string)
	if !ok {
		return nil, fmt.Errorf("no down playbook in sp")
	}
	playbookDelete, ok := ansibleParams["playbook_vpn_delete"].(string)
	if !ok {
		return nil, fmt.Errorf("no delete playbook in sp")
	}
	playbookSniff, ok := ansibleParams["playbook_vpn_sniff"].(string)
	if !ok {
		return nil, fmt.Errorf("no sniff playbook in sp")
	}
	baseUrl, ok := ansibleParams["nocloud_base_url"].(string)
	if !ok {
		return nil, fmt.Errorf("no nocloud base url in sp")
	}
	var playbooksChain []string
	action, ok := data["action"]
	if !ok || action == nil || action.GetStringValue() == "" {
		return nil, fmt.Errorf("no action provided")
	}
	switch action.GetStringValue() {
	case "create":
		playbooksChain = []string{playbookUp}
	case "stop":
		playbooksChain = []string{playbookDown}
	case "start":
		playbooksChain = []string{playbookStart}
	case "hard_reset":
		playbooksChain = []string{playbookDelete, playbookUp}
		if val, ok := data["host"]; ok && val.GetStringValue() != "" {
			if inst != nil {
				inst.Config = map[string]*structpb.Value{
					"username": data["username"],
					"password": data["password"],
					"host":     data["host"],
					"port":     data["port"],
				}
			}
		}
	case "sniff":
		playbooksChain = []string{playbookSniff}
		if inst == nil {
			inst = &ipb.Instance{
				Uuid: "no_uuid",
				Config: map[string]*structpb.Value{
					"username": data["username"],
					"password": data["password"],
					"host":     data["host"],
					"port":     data["port"],
				},
			}
		}
		if val, ok := data["host"]; ok && val.GetStringValue() != "" {
			if inst != nil {
				inst.Config = map[string]*structpb.Value{
					"username": data["username"],
					"password": data["password"],
					"host":     data["host"],
					"port":     data["port"],
				}
			}
		}
	case "restart":
		playbooksChain = []string{playbookDown, playbookStart}
	case "delete":
		playbooksChain = []string{playbookDelete}
	default:
		return nil, fmt.Errorf("invalid action provided")
	}
	if inst == nil || inst.Config == nil {
		return nil, fmt.Errorf("no config data provided")
	}
	if len(playbooksChain) == 0 {
		return nil, fmt.Errorf("no playbooks to play")
	}
	log = log.Named("VpnAction").With(zap.String("instance", inst.GetUuid()), zap.String("action", action.GetStringValue()))
	// Get hosts data (based on driver)
	var host, username, password string
	var port *string
	username = inst.GetConfig()["username"].GetStringValue()
	password = inst.GetConfig()["password"].GetStringValue()
	host, port, _ = findInstanceHostPort(inst)
	//
	if host == "" {
		return nil, fmt.Errorf("no host")
	}
	if username == "" {
		return nil, fmt.Errorf("no username")
	}
	if password == "" {
		return nil, fmt.Errorf("no password")
	}
	ansibleInstance := &ansible.Instance{
		Uuid: inst.GetUuid(),
		Host: host,
		Port: port,
		User: &username,
		Pass: &password,
	}
	instToken, err := auth.MakeTokenInstance(inst.GetUuid())
	if err != nil {
		return nil, fmt.Errorf("failed to issue instance token: %w", err)
	}
	runPlaybook := func(playbook string) (errs []AnsibleError, err error) {
		log := log.With(zap.String("playbook", playbook))
		create, err := client.Create(ctx, &ansible.CreateRunRequest{
			Run: &ansible.Run{
				Instances: []*ansible.Instance{
					ansibleInstance,
				},
				PlaybookUuid: playbook,
				Vars: map[string]string{
					"INSTANCE_TOKEN":       instToken,
					"POST_STATE_URL":       path.Join(baseUrl, "edge/post_state"),
					"POST_CONFIG_DATA_URL": path.Join(baseUrl, "edge/post_config_data"),
				},
			},
		})
		if err != nil {
			return errs, fmt.Errorf("failed to create new runnable instance: %w", err)
		}
		resp, err := client.Exec(ctx, &ansible.ExecRunRequest{
			Uuid:       create.GetUuid(),
			WaitFinish: true,
		})
		if err != nil {
			return errs, fmt.Errorf("failed to execute: %w", err)
		}
		if resp.GetStatus() == "failed" {
			for _, e := range resp.GetError() {
				log.Debug("Got ansible error", zap.String("host", e.Host), zap.String("message", e.GetError()))
				msg := fmt.Sprintf("Host: %s Message: %s", e.Host, e.Error)
				if e.GetError() == "UNREACHABLE" {
					errs = append(errs, AnsibleError{Code: codeUnreachable,
						Message: msg, UserMessage: "No access to remote host."})
				} else if strings.Contains(e.GetError(), "UNSUPPORTED_OS") {
					errs = append(errs, AnsibleError{Code: codeUnsupportedOS,
						Message: msg, UserMessage: "Remote machine has unsupported operating system."})
				} else if strings.Contains(e.GetError(), "STOPPED") {
					errs = append(errs, AnsibleError{Code: codeStopped,
						Message: msg, UserMessage: "VPN stopped."})
				} else {
					errs = append(errs, AnsibleError{Code: codeInternal,
						Message: msg, UserMessage: "Internal error. Try again later or contact support."})
				}
			}
		} else if resp.GetStatus() != "successful" {
			log.Error("Status is not successful", zap.String("status", resp.GetStatus()))
		}
		return errs, nil
	}
	for _, p := range playbooksChain {
		if ansErrs, pbErr := runPlaybook(p); pbErr != nil || len(ansErrs) > 0 {
			b, err := json.Marshal(ansErrs)
			if err != nil {
				log.Error("Failed to construct marshal errors", zap.Error(err))
			}
			s := &structpb.ListValue{}
			if err = protojson.Unmarshal(b, s); err != nil {
				log.Error("Failed to unmarshal to structpb.ListValue", zap.Error(err))
			}
			return &ipb.InvokeResponse{
				Result: false,
				Meta: map[string]*structpb.Value{
					"errors": structpb.NewListValue(s),
				},
			}, pbErr
		}
	}
	return &ipb.InvokeResponse{
		Result: true,
	}, nil
}
func findInstanceHostPort(inst *ipb.Instance) (string, *string, error) {
	var port *string
	if inst == nil {
		return "", port, fmt.Errorf("instance is nil")
	}
	if val, ok := inst.GetConfig()["host"]; ok && val.GetStringValue() != "" {
		if p, ok := inst.GetConfig()["port"]; ok && p.GetStringValue() != "" {
			pVal := p.GetStringValue()
			port = &pVal
		}
		return val.GetStringValue(), port, nil
	}
	for _, i := range inst.GetState().GetInterfaces() {
		if host, ok := i.GetData()["host"]; ok && host != "" {
			if p := i.GetData()["port"]; p != "" {
				port = &p
			}
			return host, port, nil
		}
	}
	return "", port, fmt.Errorf("not found")
}
