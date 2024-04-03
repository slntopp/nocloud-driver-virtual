package server

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/slntopp/nocloud-proto/billing"
	epb "github.com/slntopp/nocloud-proto/events"
	"github.com/slntopp/nocloud-proto/instances"
	statespb "github.com/slntopp/nocloud-proto/states"
	statusespb "github.com/slntopp/nocloud-proto/statuses"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

type ExpiryDiff struct {
	Timestamp int64
	Days      int64
}

var notificationsPeriods = []ExpiryDiff{
	{0, 0},
	{86400, 1},
	{172800, 2},
	{259200, 3},
	{604800, 7},
	{1296000, 15},
	{2592000, 30},
}

func (s *VirtualDriver) _handleInstanceBilling(i *instances.Instance, balance *float64) {
	log := s.log.Named("BillingHandler").Named(i.GetUuid())
	log.Debug("Initializing")

	status := i.GetStatus()

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
	config := i.GetConfig()

	var records []*billing.Record

	var skipPayment []any

	if config != nil {
		skipPayment = config["skip_next_payment"].GetListValue().AsSlice()
	}

	if plan.Kind == billing.PlanKind_STATIC {
		product := i.GetBillingPlan().GetProducts()[i.GetProduct()]

		var iProduct any = i.GetProduct()

		var last int64
		var priority billing.Priority

		var configAddons, productAddons []any

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
				_, ok := i.Data[resource.GetKey()+"_last_monitoring"]

				if ok {
					last = int64(i.Data[resource.GetKey()+"_last_monitoring"].GetNumberValue())
				} else {
					last = time.Now().Unix()
				}

				if resource.GetPeriod() == 0 {
					if !ok {
						if !slices.Contains(skipPayment, key) {
							records = append(records, handleOneTimeResourcePayment(log, i, resource, last)...)
						}
						i.Data[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(last))
					}
				} else {
					capRec, last := handleCapacityBilling(log, i, resource, last)
					if ok || (!ok && !slices.Contains(skipPayment, key)) {
						records = append(records, capRec...)
					}
					i.Data[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(last))
				}
			}
		}

		_, ok := i.Data["last_monitoring"]

		if ok {
			last = int64(i.Data["last_monitoring"].GetNumberValue())
			priority = billing.Priority_NORMAL
		} else {
			last = time.Now().Unix()
			priority = billing.Priority_URGENT
		}

		if product.GetPeriod() == 0 {
			if !ok {
				if !slices.Contains(skipPayment, iProduct) {
					records = append(records, handleOneTimePayment(log, i, last, priority)...)
				}
				i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))
			}
		} else {
			new, last := handleStaticBilling(log, i, last, priority)
			if len(new) != 0 {
				if ok || (!ok && !slices.Contains(skipPayment, iProduct)) {
					records = append(records, new...)
				}
				i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))
			}

			if product.GetKind() == billing.Kind_POSTPAID {
				end := last + product.GetPeriod()
				lastDay := time.Unix(last, 0).Day()
				endDay := time.Unix(end, 0).Day()

				if lastDay-endDay == 1 {
					end += 86400
				} else if lastDay-endDay == -29 {
					end += 2 * 86400
				} else if lastDay-endDay == -1 {
					end -= 86400
				} else if lastDay-endDay == -2 {
					end -= 2 * 86400
				}

				i.Data["next_payment_date"] = structpb.NewNumberValue(float64(end))
			} else {
				i.Data["next_payment_date"] = structpb.NewNumberValue(float64(last))
			}
		}
	}

	if len(records) != 0 && status == statusespb.NoCloudStatus_SUS {
		log.Debug("SUS")
		if i.GetState().GetState() != statespb.NoCloudState_SUSPENDED {
			go s.HandlePublishInstanceState(&statespb.ObjectState{
				Uuid: i.GetUuid(),
				State: &statespb.State{
					State: statespb.NoCloudState_SUSPENDED,
				},
			})

			go s.HandlePublishEvent(&epb.Event{
				Uuid: i.GetUuid(),
				Key:  "instance_suspended",
				Data: map[string]*structpb.Value{},
			})
		}
	} else {
		log.Debug("NOT SUS")
		var price float64
		for _, rec := range records {
			price += rec.GetTotal()
		}

		if price > *balance {
			if i.GetState().GetState() != statespb.NoCloudState_SUSPENDED {
				go s.HandlePublishInstanceState(&statespb.ObjectState{
					Uuid: i.GetUuid(),
					State: &statespb.State{
						State: statespb.NoCloudState_SUSPENDED,
					},
				})

				go s.HandlePublishEvent(&epb.Event{
					Uuid: i.GetUuid(),
					Key:  "instance_suspended",
					Data: map[string]*structpb.Value{},
				})
			}
			return
		}

		*balance -= price
		if i.GetState().GetState() == statespb.NoCloudState_SUSPENDED {
			go s.HandlePublishInstanceState(&statespb.ObjectState{
				Uuid: i.GetUuid(),
				State: &statespb.State{
					State: statespb.NoCloudState_RUNNING,
				},
			})

			go s.HandlePublishEvent(&epb.Event{
				Uuid: i.GetUuid(),
				Key:  "instance_unsuspended",
				Data: map[string]*structpb.Value{},
			})
		}
		s._handleEvent(i)
		s.HandlePublishRecords(records)
		s.HandlePublishInstanceData(&instances.ObjectData{
			Uuid: i.GetUuid(), Data: i.Data,
		})
	}
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
		product := i.GetBillingPlan().GetProducts()[i.GetProduct()]

		if product.GetPeriod() == 0 {
			return
		}

		now := time.Now().Unix()
		lastMonitoringValue := int64(lastMonitoring.GetNumberValue())

		suspendedManually := i.GetData()["suspended_manually"].GetBoolValue()

		if product.GetKind() == billing.Kind_POSTPAID {
			if now > lastMonitoringValue+product.GetPeriod() && i.GetState().GetState() != statespb.NoCloudState_SUSPENDED {
				go s.HandlePublishInstanceState(&statespb.ObjectState{
					Uuid: i.GetUuid(),
					State: &statespb.State{
						State: statespb.NoCloudState_SUSPENDED,
					},
				})

				go s.HandlePublishEvent(&epb.Event{
					Uuid: i.GetUuid(),
					Key:  "instance_suspended",
					Data: map[string]*structpb.Value{},
				})
			} else if now <= lastMonitoringValue+product.GetPeriod() && i.GetState().GetState() == statespb.NoCloudState_SUSPENDED && !suspendedManually {
				go s.HandlePublishInstanceState(&statespb.ObjectState{
					Uuid: i.GetUuid(),
					State: &statespb.State{
						State: statespb.NoCloudState_RUNNING,
					},
				})

				go s.HandlePublishEvent(&epb.Event{
					Uuid: i.GetUuid(),
					Key:  "instance_unsuspended",
					Data: map[string]*structpb.Value{},
				})
			}

			end := lastMonitoringValue + product.GetPeriod()

			if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				lastDay := time.Unix(lastMonitoringValue, 0).Day()
				endDay := time.Unix(end, 0).Day()

				if lastDay-endDay == 1 {
					end += 86400
				} else if lastDay-endDay == -29 {
					end += 2 * 86400
				} else if lastDay-endDay == -1 {
					end -= 86400
				} else if lastDay-endDay == -2 {
					end -= 2 * 86400
				}
			}

			i.Data["next_payment_date"] = structpb.NewNumberValue(float64(end))
		} else {
			if now > lastMonitoringValue && i.GetState().GetState() != statespb.NoCloudState_SUSPENDED {
				go s.HandlePublishInstanceState(&statespb.ObjectState{
					Uuid: i.GetUuid(),
					State: &statespb.State{
						State: statespb.NoCloudState_SUSPENDED,
					},
				})

				go s.HandlePublishEvent(&epb.Event{
					Uuid: i.GetUuid(),
					Key:  "instance_suspended",
					Data: map[string]*structpb.Value{},
				})
			} else if now <= lastMonitoringValue && i.GetState().GetState() == statespb.NoCloudState_SUSPENDED && !suspendedManually {
				go s.HandlePublishInstanceState(&statespb.ObjectState{
					Uuid: i.GetUuid(),
					State: &statespb.State{
						State: statespb.NoCloudState_RUNNING,
					},
				})

				go s.HandlePublishEvent(&epb.Event{
					Uuid: i.GetUuid(),
					Key:  "instance_unsuspended",
					Data: map[string]*structpb.Value{},
				})
			}
			i.Data["next_payment_date"] = structpb.NewNumberValue(float64(lastMonitoringValue))
		}

		s._handleEvent(i)
		s.HandlePublishInstanceData(&instances.ObjectData{
			Uuid: i.GetUuid(),
			Data: i.Data,
		})
	} else {
		plan := i.GetBillingPlan()
		if plan == nil {
			log.Debug("Instance has no Billing Plan")
			return
		}

		var records []*billing.Record

		if plan.Kind == billing.PlanKind_STATIC {
			product := i.GetBillingPlan().GetProducts()[i.GetProduct()]

			var last int64
			var priority billing.Priority

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
					_, ok := i.Data[resource.GetKey()+"_last_monitoring"]

					if ok {
						last = int64(i.Data[resource.GetKey()+"_last_monitoring"].GetNumberValue())
					} else {
						last = time.Now().Unix()
					}

					if resource.GetPeriod() == 0 {
						if !ok {
							records = append(records, handleOneTimeResourcePayment(log, i, resource, last)...)
							i.Data[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(last))
						} else {
							capRec, last := handleCapacityBilling(log, i, resource, last)
							records = append(records, capRec...)
							i.Data[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(last))
						}
					}
				}
			}

			_, ok := i.Data["last_monitoring"]

			if ok {
				last = int64(i.Data["last_monitoring"].GetNumberValue())
				priority = billing.Priority_NORMAL
			} else {
				last = time.Now().Unix()
				priority = billing.Priority_URGENT
			}

			if product.GetPeriod() == 0 {
				if !ok {
					records = append(records, handleOneTimePayment(log, i, last, priority)...)
					i.Data["last_monitoring"] = structpb.NewNumberValue(float64(last))
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
			}
		}

		log.Debug("Resulting billing", zap.Any("records", records))
		s.HandlePublishRecords(records)
		s._handleEvent(i)
		s.HandlePublishInstanceData(&instances.ObjectData{
			Uuid: i.GetUuid(), Data: i.Data,
		})
	}
}

func (s *VirtualDriver) _handleRenewBilling(inst *instances.Instance) error {
	log := s.log.Named("Manual renew")
	instData := inst.GetData()
	instProduct := inst.GetProduct()
	billingPlan := inst.GetBillingPlan()

	product := billingPlan.GetProducts()[instProduct]

	if product.GetPeriod() == 0 {
		return errors.New("period is 0")
	}

	log.Debug("Product", zap.Any("pr", instProduct), zap.Any("Product", product))

	log.Debug("Init data", zap.Any("data", instData))

	lastMonitoring, ok := instData["last_monitoring"]
	if !ok {
		log.Error("No last monitoring")
		return errors.New("no last monitoring")
	}
	lastMonitoringValue := int64(lastMonitoring.GetNumberValue())

	start := lastMonitoringValue
	end := start + product.GetPeriod()

	if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {

		lastDay := time.Unix(start, 0).Day()
		endDay := time.Unix(end, 0).Day()

		if lastDay-endDay == 1 {
			end += 86400
		} else if lastDay-endDay == -29 {
			end += 2 * 86400
		} else if lastDay-endDay == -1 {
			end -= 86400
		} else if lastDay-endDay == -2 {
			end -= 2 * 86400
		}
	}

	var records []*billing.Record

	log.Debug("lm", zap.Any("before", lastMonitoringValue))
	records = append(records, &billing.Record{
		Start:    start,
		End:      end,
		Exec:     time.Now().Unix(),
		Priority: billing.Priority_URGENT,
		Instance: inst.GetUuid(),
		Product:  inst.GetProduct(),
		Total:    product.GetPrice(),
	})
	instData["last_monitoring"] = structpb.NewNumberValue(float64(end))

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

				start := int64(lmVal)
				end := start + resource.GetPeriod()

				if resource.GetPeriodKind() != billing.PeriodKind_DEFAULT {

					lastDay := time.Unix(start, 0).Day()
					endDay := time.Unix(end, 0).Day()

					if lastDay-endDay == 1 {
						end += 86400
					} else if lastDay-endDay == -29 {
						end += 2 * 86400
					} else if lastDay-endDay == -1 {
						end -= 86400
					} else if lastDay-endDay == -2 {
						end -= 2 * 86400
					}
				}

				records = append(records, &billing.Record{
					Start:    start,
					End:      end,
					Exec:     time.Now().Unix(),
					Priority: billing.Priority_URGENT,
					Instance: inst.GetUuid(),
					Resource: resource.GetKey(),
					Total:    resource.GetPrice(),
				})
				instData[resource.GetKey()+"_last_monitoring"] = structpb.NewNumberValue(float64(end))
			}
		}
	}

	log.Debug("Final data", zap.Any("data", instData))
	log.Debug("records", zap.Any("recs", records))

	s.HandlePublishRecords(records)
	s.HandlePublishEvent(&epb.Event{
		Type: "instance_renew",
		Uuid: inst.GetUuid(),
		Data: map[string]*structpb.Value{},
	})
	s.HandlePublishInstanceData(&instances.ObjectData{
		Uuid: inst.GetUuid(),
		Data: instData,
	})
	return nil
}

func (s *VirtualDriver) _handleEvent(i *instances.Instance) {
	log := s.log.Named("BusEvent").Named(i.GetUuid())
	log.Debug("Get event", zap.String("uuid", i.GetUuid()))
	if i.GetStatus() == statusespb.NoCloudStatus_DEL {
		return
	}

	data := i.GetData()
	now := time.Now().Unix()

	last_monitoring, ok := data["last_monitoring"]
	if !ok {
		return
	}

	last_monitoring_value := int64(last_monitoring.GetNumberValue())

	productName := i.GetProduct()

	products := i.GetBillingPlan().GetProducts()
	product, ok := products[productName]

	if !ok {
		return
	}

	productKind := product.GetKind()
	period := product.GetPeriod()

	var diff int64
	var expirationDate int64

	if productKind == billing.Kind_PREPAID {
		diff = last_monitoring_value - now
		expirationDate = last_monitoring_value
	} else {
		diff = last_monitoring_value + period - now
		expirationDate = last_monitoring_value + period
	}

	log.Debug("Diff", zap.Any("d", diff))

	unix := time.Unix(expirationDate, 0)
	year, month, day := unix.Date()
	for _, val := range notificationsPeriods {
		if diff <= val.Timestamp {

			if val.Timestamp == period {
				break
			}

			notification_period, ok := data["notification_period"]
			if !ok {
				data["notification_period"] = structpb.NewNumberValue(float64(val.Days))
				go s.HandlePublishEvent(&epb.Event{
					Uuid: i.GetUuid(),
					Key:  "expiry_notification",
					Data: map[string]*structpb.Value{
						"period":  structpb.NewNumberValue(float64(val.Days)),
						"product": structpb.NewStringValue(i.GetProduct()),
						"date":    structpb.NewStringValue(fmt.Sprintf("%d/%d/%d", day, month, year)),
					},
				})
				continue
			}

			if val.Days != int64(notification_period.GetNumberValue()) {
				data["notification_period"] = structpb.NewNumberValue(float64(val.Days))
				go s.HandlePublishEvent(&epb.Event{
					Uuid: i.GetUuid(),
					Key:  "expiry_notification",
					Data: map[string]*structpb.Value{
						"period":  structpb.NewNumberValue(float64(val.Days)),
						"product": structpb.NewStringValue(i.GetProduct()),
						"date":    structpb.NewStringValue(fmt.Sprintf("%d/%d/%d", day, month, year)),
					},
				})
			}
			break
		}
	}
	log.Debug("Data", zap.Any("d", data))
	i.Data = data
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
			if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				lastDay := time.Unix(last, 0).Day()
				endDay := time.Unix(end, 0).Day()

				if lastDay-endDay == 1 {
					end += 86400
				} else if lastDay-endDay == -29 {
					end += 2 * 86400
				} else if lastDay-endDay == -1 {
					end -= 86400
				} else if lastDay-endDay == -2 {
					end -= 2 * 86400
				}
			}
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
			if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				lastDay := time.Unix(last, 0).Day()
				endDay := time.Unix(end, 0).Day()

				if lastDay-endDay == 1 {
					end += 86400
				} else if lastDay-endDay == -29 {
					end += 2 * 86400
				} else if lastDay-endDay == -1 {
					end -= 86400
				} else if lastDay-endDay == -2 {
					end -= 2 * 86400
				}
			}
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
			if res.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				lastDay := time.Unix(last, 0).Day()
				endDay := time.Unix(end, 0).Day()

				if lastDay-endDay == 1 {
					end += 86400
				} else if lastDay-endDay == -29 {
					end += 2 * 86400
				} else if lastDay-endDay == -1 {
					end -= 86400
				} else if lastDay-endDay == -2 {
					end -= 2 * 86400
				}
			}
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
			if res.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				lastDay := time.Unix(last, 0).Day()
				endDay := time.Unix(end, 0).Day()

				if lastDay-endDay == 1 {
					end += 86400
				} else if lastDay-endDay == -29 {
					end += 2 * 86400
				} else if lastDay-endDay == -1 {
					end -= 86400
				} else if lastDay-endDay == -2 {
					end -= 2 * 86400
				}
			}
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
