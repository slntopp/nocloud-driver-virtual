package utils

import (
	"github.com/slntopp/nocloud-proto/instances"
	i "github.com/slntopp/nocloud/pkg/instances"
	"google.golang.org/protobuf/types/known/structpb"
)

func SendActualMonitoringData(copiedData map[string]*structpb.Value, actualData map[string]*structpb.Value, instance string, publisher i.Pub) {
	if val, ok := actualData["last_monitoring"]; ok {
		copiedData["actual_last_monitoring"] = val
	}
	if val, ok := actualData["next_payment_date"]; ok {
		copiedData["actual_next_payment_date"] = val
	}
	_, _ = publisher(&instances.ObjectData{
		Uuid: instance,
		Data: copiedData,
	})
}
