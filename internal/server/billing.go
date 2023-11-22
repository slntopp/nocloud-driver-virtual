package server

import (
	"math"
	"slices"
	"time"

	"github.com/slntopp/nocloud-proto/billing"
	"github.com/slntopp/nocloud-proto/instances"
	statespb "github.com/slntopp/nocloud-proto/states"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

func (s *VirtualDriver) _handleInstanceBilling(i *instances.Instance) {
	log := s.log.Named("BillingHandler").Named(i.GetUuid())
	log.Debug("Initializing")

	if statespb.NoCloudState_PENDING == i.GetState().GetState() {
		log.Info("Instance state is init. No instance billing", zap.String("uuid", i.GetUuid()))
		return
	}

	plan := i.GetBillingPlan()
	if plan == nil {
		log.Debug("Instance has no Billing Plan")
		return
	}

	if i.GetData() == nil {
		log.Warn("Instance data is not initialized")
		i.Data = make(map[string]*structpb.Value)
	}

	var records []*billing.Record

	if plan.Kind == billing.PlanKind_STATIC {
		var last int64
		var priority billing.Priority
		if _, ok := i.Data["last_monitoring"]; ok {
			last = int64(i.Data["last_monitoring"].GetNumberValue())
			priority = billing.Priority_NORMAL
		} else {
			last = time.Now().Unix()
			priority = billing.Priority_URGENT
		}
		new, last := handleStaticBilling(log, i, last, priority)
		if len(new) != 0 {
			records = append(records, new...)
			i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))
		}

		product := i.GetBillingPlan().GetProducts()[i.GetProduct()]
		if product.GetKind() == billing.Kind_POSTPAID {
			i.Data["next_payment_date"] = structpb.NewNumberValue(float64(last + product.GetPeriod()))
		} else {
			i.Data["next_payment_date"] = structpb.NewNumberValue(float64(last))
		}

		var configAddons, productAddons []any
		config := i.GetConfig()
		if config != nil {
			configAddons = config["addons"].GetListValue().AsSlice()
		}

		meta := product.GetMeta()
		if meta != nil {
			productAddons = meta["addons"].GetListValue().AsSlice()
		}

		for _, resource := range plan.Resources {
			var key any = resource.GetKey()
			if slices.Contains(configAddons, key) && slices.Contains(productAddons, key) {
				if _, ok := i.Data[resource.GetKey()+"_last_monitoring"]; ok {
					last = int64(i.Data[resource.GetKey()+"_last_monitoring"].GetNumberValue())
				} else {
					last = time.Now().Unix()
				}
				recs, last := handleCapacityBilling(log, i, resource, last)
				if len(recs) != 0 {
					records = append(records, recs...)
					i.Data[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(last))
				}

				if resource.GetKind() == billing.Kind_POSTPAID {
					i.Data[resource.GetKey()+"_next_payment_date"] = structpb.NewNumberValue(float64(last + resource.GetPeriod()))
				} else {
					i.Data[resource.GetKey()+"_next_payment_date"] = structpb.NewNumberValue(float64(last))
				}
			}
		}
	}

	log.Debug("Resulting billing", zap.Any("records", records))
	s.HandlePublishRecords(records)
	s.HandlePublishInstanceData(&instances.ObjectData{
		Uuid: i.GetUuid(), Data: i.Data,
	})
}

func handleStaticBilling(log *zap.Logger, i *instances.Instance, last int64, priority billing.Priority) ([]*billing.Record, int64) {
	log.Debug("Handling Static Billing", zap.Int64("last", last))
	product, ok := i.BillingPlan.Products[*i.Product]
	if !ok {
		log.Warn("Product not found", zap.String("product", *i.Product))
		return nil, last
	}

	var records []*billing.Record
	if product.Kind == billing.Kind_POSTPAID {
		log.Debug("Handling Postpaid Billing", zap.Any("product", product))
		for end := last + product.Period; end <= time.Now().Unix(); end += product.Period {
			records = append(records, &billing.Record{
				Product:  *i.Product,
				Instance: i.GetUuid(),
				Start:    last, End: end, Exec: last,
				Priority: billing.Priority_NORMAL,
				Total:    math.Round(product.Price*100) / 100.0,
			})
		}
	} else {
		end := last + product.Period
		log.Debug("Handling Prepaid Billing", zap.Any("product", product), zap.Int64("end", end), zap.Int64("now", time.Now().Unix()))
		for ; last <= time.Now().Unix(); end += product.Period {
			records = append(records, &billing.Record{
				Product:  *i.Product,
				Instance: i.GetUuid(),
				Start:    last, End: end, Exec: last,
				Priority: priority,
				Total:    math.Round(product.Price*100) / 100.0,
			})
			last = end
		}
	}

	return records, last
}

func handleCapacityBilling(log *zap.Logger, i *instances.Instance, res *billing.ResourceConf, last int64) ([]*billing.Record, int64) {
	var records []*billing.Record

	if res.Kind == billing.Kind_POSTPAID {
		for end := last + res.Period; end <= time.Now().Unix(); end += res.Period {
			records = append(records, &billing.Record{
				Resource: res.Key,
				Instance: i.GetUuid(),
				Start:    last, End: end,
				Exec:  last,
				Total: res.GetPrice(),
			})
			last = end
		}
	} else {
		for end := last + res.Period; last <= time.Now().Unix(); end += res.Period {
			records = append(records, &billing.Record{
				Resource: res.Key,
				Instance: i.GetUuid(),
				Priority: billing.Priority_URGENT,
				Start:    last, End: end, Exec: last,
				Total: res.GetPrice(),
			})
			last = end
		}
	}

	return records, last
}
