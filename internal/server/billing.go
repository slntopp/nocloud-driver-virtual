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

		_, ok := i.Data["last_monitoring"]

		if ok {
			last = int64(i.Data["last_monitoring"].GetNumberValue())
			priority = billing.Priority_NORMAL
		} else {
			last = time.Now().Unix()
			priority = billing.Priority_URGENT
		}

		product := i.GetBillingPlan().GetProducts()[i.GetProduct()]

		if product.GetPeriod() == 0 {
			if !ok {
				records = append(records, handleOneTimePayment(log, i, last, priority)...)
				i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))

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

						records = append(records, handleOneTimeResourcePayment(log, i, resource, last)...)
						i.Data[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(last))
					}
				}
			}
		} else {
			new, last := handleStaticBilling(log, i, last, priority)
			if len(new) != 0 {
				records = append(records, new...)
				i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))
			}

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
	}

	log.Debug("Resulting billing", zap.Any("records", records))
	s.HandlePublishRecords(records)
	s.HandlePublishInstanceData(&instances.ObjectData{
		Uuid: i.GetUuid(), Data: i.Data,
	})
}

func (s *VirtualDriver) _handleNonRegularBilling(i *instances.Instance) {
	log := s.log.Named("NonReg").Named(i.GetUuid())
	log.Debug("Initializing")

	if statespb.NoCloudState_PENDING == i.GetState().GetState() {
		log.Info("Instance state is init. No instance billing", zap.String("uuid", i.GetUuid()))
		return
	}

	if i.GetData() == nil {
		log.Warn("Instance data is not initialized")
		i.Data = make(map[string]*structpb.Value)
	}

	if lastMonitoring, ok := i.GetData()["last_monitoring"]; ok {
		now := time.Now().Unix()
		lastMonitoringValue := int64(lastMonitoring.GetNumberValue())

		suspendedManually := i.GetData()["suspended_manually"].GetBoolValue()

		if now > lastMonitoringValue && i.GetState().GetState() != statespb.NoCloudState_SUSPENDED {
			go s.HandlePublishInstanceState(&statespb.ObjectState{
				Uuid: i.GetUuid(),
				State: &statespb.State{
					State: statespb.NoCloudState_SUSPENDED,
				},
			})
		} else if now <= lastMonitoringValue && i.GetState().GetState() == statespb.NoCloudState_SUSPENDED && !suspendedManually {
			go s.HandlePublishInstanceState(&statespb.ObjectState{
				Uuid: i.GetUuid(),
				State: &statespb.State{
					State: statespb.NoCloudState_RUNNING,
				},
			})
		}
	} else {
		plan := i.GetBillingPlan()
		if plan == nil {
			log.Debug("Instance has no Billing Plan")
			return
		}

		var records []*billing.Record

		if plan.Kind == billing.PlanKind_STATIC {
			var last int64
			var priority billing.Priority

			_, ok := i.Data["last_monitoring"]

			if ok {
				last = int64(i.Data["last_monitoring"].GetNumberValue())
				priority = billing.Priority_NORMAL
			} else {
				last = time.Now().Unix()
				priority = billing.Priority_URGENT
			}

			product := i.GetBillingPlan().GetProducts()[i.GetProduct()]

			if product.GetPeriod() == 0 {
				if !ok {
					records = append(records, handleOneTimePayment(log, i, last, priority)...)
					i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))

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

							records = append(records, handleOneTimeResourcePayment(log, i, resource, last)...)
							i.Data[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(last))
						}
					}
				}
			} else {
				new, last := handleStaticBilling(log, i, last, priority)
				if len(new) != 0 {
					records = append(records, new...)
					i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))
				}

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
		}

		log.Debug("Resulting billing", zap.Any("records", records))
		s.HandlePublishRecords(records)
		s.HandlePublishInstanceData(&instances.ObjectData{
			Uuid: i.GetUuid(), Data: i.Data,
		})
	}
}

func (s *VirtualDriver) _handleRenewBilling(inst *instances.Instance) {
	log := s.log.Named("Manual renew")
	instData := inst.GetData()
	instProduct := inst.GetProduct()
	billingPlan := inst.GetBillingPlan()

	product := billingPlan.GetProducts()[instProduct]

	log.Debug("Product", zap.Any("pr", instProduct), zap.Any("Product", product))

	log.Debug("Init data", zap.Any("data", instData))

	lastMonitoring, ok := instData["last_monitoring"]
	if !ok {
		log.Error("No last monitoring")
		return
	}
	lastMonitoringValue := int64(lastMonitoring.GetNumberValue())

	var records []*billing.Record

	log.Debug("lm", zap.Any("before", lastMonitoringValue))
	records = append(records, &billing.Record{
		Start:    lastMonitoringValue,
		End:      lastMonitoringValue + product.GetPeriod(),
		Exec:     time.Now().Unix(),
		Priority: billing.Priority_URGENT,
		Instance: inst.GetUuid(),
		Product:  inst.GetProduct(),
		Total:    product.GetPrice(),
	})
	lastMonitoringValue += product.GetPeriod()
	log.Debug("lm", zap.Any("after", lastMonitoringValue))
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
				log.Debug("lm", zap.Any("before", lmVal))
				records = append(records, &billing.Record{
					Start:    int64(lmVal),
					End:      int64(lmVal) + product.GetPeriod(),
					Exec:     time.Now().Unix(),
					Priority: billing.Priority_URGENT,
					Instance: inst.GetUuid(),
					Resource: resource.GetKey(),
					Total:    resource.GetPrice(),
				})
				lmVal += float64(resource.GetPeriod())
				log.Debug("lm", zap.Any("before", lmVal))
				instData[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(lmVal)
			}
		}
	}

	log.Debug("Final data", zap.Any("data", instData))
	log.Debug("records", zap.Any("recs", records))

	s.HandlePublishRecords(records)

	s.HandlePublishInstanceData(&instances.ObjectData{
		Uuid: inst.GetUuid(),
		Data: instData,
	})
}

func handleOneTimePayment(log *zap.Logger, i *instances.Instance, last int64, priority billing.Priority) []*billing.Record {
	log.Debug("Handling Static Billing", zap.Int64("last", last))
	product, ok := i.BillingPlan.Products[*i.Product]
	if !ok {
		log.Warn("Product not found", zap.String("product", *i.Product))
		return nil
	}

	var records []*billing.Record

	records = append(records, &billing.Record{
		Product:  *i.Product,
		Instance: i.GetUuid(),
		Start:    last, End: last + 1, Exec: last,
		Priority: priority,
		Total:    math.Round(product.Price*100) / 100.0,
	})

	return records
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

func handleOneTimeResourcePayment(log *zap.Logger, i *instances.Instance, res *billing.ResourceConf, last int64) []*billing.Record {
	var records []*billing.Record

	records = append(records, &billing.Record{
		Resource: res.Key,
		Instance: i.GetUuid(),
		Start:    last, End: last + 1,
		Exec:  last,
		Total: res.GetPrice(),
	})

	return records
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
