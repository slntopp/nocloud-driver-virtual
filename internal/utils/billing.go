package utils

import (
	"github.com/slntopp/nocloud-proto/instances"
	i "github.com/slntopp/nocloud/pkg/instances"
	"google.golang.org/protobuf/types/known/structpb"
)

func SendActualMonitoringData(copiedData map[string]*structpb.Value, actualData map[string]*structpb.Value, instance string, publisher i.Pub) {
	copiedData["actual_last_monitoring"] = actualData["last_monitoring"]
	copiedData["actual_next_payment_date"] = actualData["next_payment_date"]
	_, _ = publisher(&instances.ObjectData{
		Uuid: instance,
		Data: copiedData,
	})
}
