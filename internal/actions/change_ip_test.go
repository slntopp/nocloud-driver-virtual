package actions

import (
	"testing"

	ipb "github.com/slntopp/nocloud-proto/instances"
	stpb "github.com/slntopp/nocloud-proto/states"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func list(ips ...any) *structpb.Value {
	v, _ := structpb.NewList(ips)
	return structpb.NewListValue(v)
}

func TestChangeIPWritesNetworking(t *testing.T) {
	inst := &ipb.Instance{
		Uuid: "inst",
		State: &stpb.State{
			State: stpb.NoCloudState_RUNNING,
			Meta:  map[string]*structpb.Value{"keep": structpb.NewStringValue("me")},
		},
	}
	var published *stpb.ObjectState
	pub := func(msg *stpb.ObjectState) (int, error) { published = msg; return 0, nil }

	_, err := ChangeIP(zap.NewNop(), pub, nil, inst, map[string]*structpb.Value{
		"public":  list("1.2.3.4", " 1.2.3.4 ", "0.0.0.0", "", "5.6.7.8/32"),
		"private": list("10.0.0.1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if published == nil || published.GetUuid() != "inst" {
		t.Fatal("state was not published")
	}
	st := published.GetState()
	if st.GetState() != stpb.NoCloudState_RUNNING {
		t.Fatalf("state changed to %v", st.GetState())
	}
	if st.GetMeta()["keep"].GetStringValue() != "me" {
		t.Fatal("existing meta was dropped")
	}
	net := st.GetMeta()["networking"].GetStructValue().AsMap()
	gotPub := net["public"].([]any)
	if len(gotPub) != 2 || gotPub[0] != "1.2.3.4" || gotPub[1] != "5.6.7.8/32" {
		t.Fatalf("public = %v", gotPub)
	}
	if p := net["private"].([]any); len(p) != 1 || p[0] != "10.0.0.1" {
		t.Fatalf("private = %v", p)
	}
}

func TestChangeIPRejectsGarbage(t *testing.T) {
	pub := func(*stpb.ObjectState) (int, error) { t.Fatal("must not publish"); return 0, nil }
	_, err := ChangeIP(zap.NewNop(), pub, nil, &ipb.Instance{}, map[string]*structpb.Value{
		"public": list("not-an-ip"),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}
