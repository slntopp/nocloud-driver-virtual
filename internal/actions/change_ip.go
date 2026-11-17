package actions

import (
	"net"
	"strings"

	"github.com/slntopp/nocloud/pkg/instances"
	"github.com/slntopp/nocloud/pkg/states"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	ipb "github.com/slntopp/nocloud-proto/instances"
	stpb "github.com/slntopp/nocloud-proto/states"
)

// ChangeIP backs the IP menu in admin-ui (same params as the IONE and proxmox drivers:
// {public: [...], private: [...]}). A virtual instance has no real network, so the addresses
// are only recorded in state.meta.networking, where the UI and the core IP search read them.
func ChangeIP(log *zap.Logger, sPub states.Pub, _ instances.Pub, inst *ipb.Instance, data map[string]*structpb.Value) (*ipb.InvokeResponse, error) {
	pub, err := parseIPs(data["public"], "public")
	if err != nil {
		return nil, err
	}
	priv, err := parseIPs(data["private"], "private")
	if err != nil {
		return nil, err
	}

	if inst.State == nil {
		inst.State = &stpb.State{}
	}
	if inst.State.Meta == nil {
		inst.State.Meta = map[string]*structpb.Value{}
	}
	inst.State.Meta["networking"] = structpb.NewStructValue(&structpb.Struct{
		Fields: map[string]*structpb.Value{
			"public":  ipsValue(pub),
			"private": ipsValue(priv),
		},
	})

	if _, err := sPub(&stpb.ObjectState{
		Uuid:  inst.GetUuid(),
		State: inst.GetState(),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to publish state: %v", err)
	}

	log.Info("IPs changed", zap.Strings("public", pub), zap.Strings("private", priv))
	return &ipb.InvokeResponse{Result: true}, nil
}

// parseIPs accepts a list of IPs or CIDRs, dropping blanks, duplicates and the 0.0.0.0
// placeholder the admin-ui "+" button inserts.
func parseIPs(v *structpb.Value, kind string) ([]string, error) {
	out := []string{}
	seen := map[string]bool{}
	for _, item := range v.GetListValue().GetValues() {
		s, ok := item.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "%s: every address must be a string", kind)
		}
		ip := strings.TrimSpace(s.StringValue)
		if ip == "" || ip == "0.0.0.0" || seen[ip] {
			continue
		}
		if net.ParseIP(ip) == nil {
			if _, _, err := net.ParseCIDR(ip); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "%s: %q is not an IP address", kind, ip)
			}
		}
		seen[ip] = true
		out = append(out, ip)
	}
	return out, nil
}

func ipsValue(ips []string) *structpb.Value {
	vals := make([]*structpb.Value, 0, len(ips))
	for _, ip := range ips {
		vals = append(vals, structpb.NewStringValue(ip))
	}
	return structpb.NewListValue(&structpb.ListValue{Values: vals})
}
