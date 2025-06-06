package server

import (
	"errors"
	"fmt"
	"github.com/slntopp/nocloud-driver-virtual/internal/utils"
	sppb "github.com/slntopp/nocloud-proto/services_providers"
	"github.com/slntopp/nocloud/pkg/nocloud/suspend_rules"
	"maps"
	"slices"
	"time"

	"github.com/slntopp/nocloud-proto/billing"
	apb "github.com/slntopp/nocloud-proto/billing/addons"
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

func (s *VirtualDriver) _handleInstanceBilling(i *instances.Instance, balance *float64, addons map[string]*apb.Addon, sp *sppb.ServicesProvider) {
	log := s.log.Named("BillingHandler").Named(i.GetUuid())
	log.Debug("Initializing")

	status := i.GetStatus()

	// Create copy of instance data
	var dataCopy = map[string]*structpb.Value{}
	maps.Copy(dataCopy, i.Data)

	if statespb.NoCloudState_PENDING == i.GetState().GetState() {
		log.Info("Instance state is init. No instance billing", zap.String("uuid", i.GetUuid()))
		return
	}

	plan := i.BillingPlan
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

		product, ok := i.BillingPlan.GetProducts()[i.GetProduct()]
		if !ok {
			log.Warn("Product not found", zap.String("product", *i.Product))
		}
		for _, addonId := range i.GetAddons() {
			addon, ok := addons[addonId]
			if !ok {
				log.Warn("Addon not found", zap.String("addon", addonId))
				continue
			}

			var (
				lm       int64
				priority billing.Priority
			)
			lmValue, ok := i.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)]
			if !ok {
				lm = i.Created
				priority = billing.Priority_URGENT
			} else {
				lm = int64(lmValue.GetNumberValue())
				priority = billing.Priority_NORMAL
			}

			recs, last := handleAddonBilling(log, i, lm, priority, addon)
			if len(recs) > 0 {
				if product.GetPeriod() == 0 {
					if !ok {
						records = append(records, recs...)
						i.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)] = structpb.NewNumberValue(float64(last))
					}
				} else {
					records = append(records, recs...)
					i.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)] = structpb.NewNumberValue(float64(last))
				}
			}
		}

		_, ok = i.Data["last_monitoring"]

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

				if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
					end = utils.AlignPaymentDate(last, end, product.GetPeriod(), i)
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

			if suspend_rules.SuspendAllowed(sp.GetSuspendRules(), time.Now().UTC()) {
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
				utils.SendActualMonitoringData(dataCopy, i.Data, i.GetUuid(), s.HandlePublishInstanceData)
			}

		}
	} else {
		log.Debug("NOT SUS")
		var price float64
		for _, rec := range records {
			if rec.Addon != "" {
				price += rec.GetTotal() * calculateAddonPrice(addons, i, rec.Addon)
			} else {
				price += rec.GetTotal() * calculateProductPrice(i, rec.Product)
			}
		}

		if price > *balance {
			if i.GetState().GetState() != statespb.NoCloudState_SUSPENDED {

				if suspend_rules.SuspendAllowed(sp.GetSuspendRules(), time.Now().UTC()) {
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

			}

			utils.SendActualMonitoringData(dataCopy, i.Data, i.GetUuid(), s.HandlePublishInstanceData)
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
		utils.SendActualMonitoringData(i.Data, i.Data, i.GetUuid(), s.HandlePublishInstanceData)
	}
}

func calculateProductPrice(i *instances.Instance, prod string) float64 {
	if i.BillingPlan == nil || i.BillingPlan.Products == nil {
		return 0
	}
	bpProd, ok := i.BillingPlan.Products[prod]
	if !ok {
		return 0
	}
	return bpProd.Price
}

func calculateAddonPrice(addons map[string]*apb.Addon, i *instances.Instance, id string) float64 {
	if i.BillingPlan == nil || i.BillingPlan.Products == nil || i.Product == nil {
		return 0
	}
	addon, ok := addons[id]
	if !ok {
		return 0
	}
	if addon.Periods == nil {
		return 0
	}
	period := i.BillingPlan.Products[*i.Product].Period
	return addon.Periods[period]
}

func (s *VirtualDriver) _handleNonRegularBilling(i *instances.Instance, addons map[string]*apb.Addon, sp *sppb.ServicesProvider) {
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

				if suspend_rules.SuspendAllowed(sp.GetSuspendRules(), time.Now().UTC()) {
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
				end = utils.AlignPaymentDate(lastMonitoringValue, end, product.GetPeriod(), i)
			}

			i.Data["next_payment_date"] = structpb.NewNumberValue(float64(end))
		} else {
			if now > lastMonitoringValue && i.GetState().GetState() != statespb.NoCloudState_SUSPENDED {

				if suspend_rules.SuspendAllowed(sp.GetSuspendRules(), time.Now().UTC()) {
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
		utils.SendActualMonitoringData(i.Data, i.Data, i.GetUuid(), s.HandlePublishInstanceData)
	} else {
		plan := i.BillingPlan
		if plan == nil {
			log.Debug("Instance has no Billing Plan")
			return
		}

		var records []*billing.Record

		if plan.Kind == billing.PlanKind_STATIC {
			product := i.GetBillingPlan().GetProducts()[i.GetProduct()]

			var last int64
			var priority billing.Priority

			product, ok := i.BillingPlan.Products[i.GetProduct()]
			if !ok {
				log.Warn("Product not found", zap.String("product", *i.Product))
			}
			for _, addonId := range i.GetAddons() {
				addon, ok := addons[addonId]
				if !ok {
					log.Warn("Addon not found", zap.String("addon", addonId))
					continue
				}

				var (
					lm       int64
					priority billing.Priority
				)
				lmValue, ok := i.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)]
				if !ok {
					lm = i.Created
					priority = billing.Priority_URGENT
				} else {
					lm = int64(lmValue.GetNumberValue())
					priority = billing.Priority_NORMAL
				}

				recs, last := handleAddonBilling(log, i, lm, priority, addon)
				if len(recs) > 0 {
					if product.GetPeriod() == 0 {
						if !ok {
							records = append(records, recs...)
							i.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)] = structpb.NewNumberValue(float64(last))
						}
					} else {
						records = append(records, recs...)
						i.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)] = structpb.NewNumberValue(float64(last))
					}
				}
			}

			_, ok = i.Data["last_monitoring"]

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
		utils.SendActualMonitoringData(i.Data, i.Data, i.GetUuid(), s.HandlePublishInstanceData)
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
		end = utils.AlignPaymentDate(start, end, product.GetPeriod(), inst)
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
		Total:    1,
	})
	instData["last_monitoring"] = structpb.NewNumberValue(float64(end))

	prod, ok := inst.BillingPlan.Products[inst.GetProduct()]
	if !ok {
		log.Warn("Product not found", zap.String("product", *inst.Product))
	}
	for _, addonId := range inst.GetAddons() {
		if prod.GetPeriod() == 0 {
			continue
		}
		var (
			lm int64
		)
		lmValue, ok := inst.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)]
		if !ok {
			continue
		} else {
			lm = int64(lmValue.GetNumberValue())
		}

		end = lm + prod.GetPeriod()
		if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
			end = utils.AlignPaymentDate(lm, end, prod.GetPeriod(), inst)
		}

		inst.Data[fmt.Sprintf("addon_%s_last_monitoring", addonId)] = structpb.NewNumberValue(float64(end))
		records = append(records, &billing.Record{
			Start:    lm,
			End:      end,
			Exec:     time.Now().Unix(),
			Priority: billing.Priority_URGENT,
			Instance: inst.GetUuid(),
			Addon:    addonId,
			Total:    1,
		})
	}

	log.Debug("Final data", zap.Any("data", instData))
	log.Debug("records", zap.Any("recs", records))

	s.HandlePublishRecords(records)
	var price float64
	for _, r := range records {
		price += r.GetTotal()
	}
	s.HandlePublishEvent(&epb.Event{
		Type: "instance_renew",
		Uuid: inst.GetUuid(),
		Data: map[string]*structpb.Value{
			"price": structpb.NewNumberValue(price),
		},
	})
	utils.SendActualMonitoringData(instData, instData, inst.GetUuid(), s.HandlePublishInstanceData)
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
	log.Debug("instance body", zap.Any("body", i))
	_, ok := i.BillingPlan.GetProducts()[i.GetProduct()]
	if !ok {
		log.Warn("Product not found", zap.String("product", *i.Product))
		return nil
	}

	var records []*billing.Record

	records = append(records, &billing.Record{
		Product:  i.GetProduct(),
		Instance: i.GetUuid(),
		Start:    last, End: last + 1, Exec: last,
		Priority: priority,
		Total:    1,
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
				end = utils.AlignPaymentDate(last, end, product.GetPeriod(), i)
			}
			records = append(records, &billing.Record{
				Product:  *i.Product,
				Instance: i.GetUuid(),
				Start:    last, End: end, Exec: last,
				Priority: billing.Priority_NORMAL,
				Total:    1,
			})
		}
	} else {
		end := last + product.Period
		log.Debug("Handling Prepaid Billing", zap.Any("product", product), zap.Int64("end", end), zap.Int64("now", time.Now().Unix()))
		for ; last <= time.Now().Unix(); end += product.Period {
			if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				end = utils.AlignPaymentDate(last, end, product.GetPeriod(), i)
			}
			records = append(records, &billing.Record{
				Product:  *i.Product,
				Instance: i.GetUuid(),
				Start:    last, End: end, Exec: last,
				Priority: priority,
				Total:    1,
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
		Total: 1,
	})

	return records
}

func handleCapacityBilling(log *zap.Logger, i *instances.Instance, res *billing.ResourceConf, last int64) ([]*billing.Record, int64) {
	var records []*billing.Record

	if res.Kind == billing.Kind_POSTPAID {
		for end := last + res.Period; end <= time.Now().Unix(); end += res.Period {
			if res.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				end = utils.AlignPaymentDate(last, end, res.GetPeriod(), i)
			}
			records = append(records, &billing.Record{
				Resource: res.Key,
				Instance: i.GetUuid(),
				Start:    last, End: end,
				Exec:  last,
				Total: 1,
			})
			last = end
		}
	} else {
		for end := last + res.Period; last <= time.Now().Unix(); end += res.Period {
			if res.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				end = utils.AlignPaymentDate(last, end, res.GetPeriod(), i)
			}
			records = append(records, &billing.Record{
				Resource: res.Key,
				Instance: i.GetUuid(),
				Priority: billing.Priority_URGENT,
				Start:    last, End: end, Exec: last,
				Total: 1,
			})
			last = end
		}
	}

	return records, last
}

func handleAddonBilling(log *zap.Logger, i *instances.Instance, last int64, priority billing.Priority, addon *apb.Addon) ([]*billing.Record, int64) {
	log.Debug("Handling Addon Billing", zap.Int64("last", last))
	product, ok := i.BillingPlan.Products[i.GetProduct()]
	if !ok {
		log.Warn("Product not found", zap.String("product", *i.Product), zap.String("addon", addon.GetUuid()))
		return nil, last
	}
	period := product.Period

	var records []*billing.Record

	// Handle one time addon payment
	if period == 0 {
		records = append(records, &billing.Record{
			Addon:    addon.GetUuid(),
			Instance: i.GetUuid(),
			Start:    last, End: last + 1, Exec: last,
			Priority: billing.Priority_URGENT,
			Total:    1,
		})
		return records, last
	}

	// Handle periodic addon payment
	if addon.Kind == apb.Kind_POSTPAID {
		log.Debug("Handling Postpaid Billing", zap.Any("addon", addon.GetUuid()))
		for end := last + period; end <= time.Now().Unix(); end += period {

			if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				end = utils.AlignPaymentDate(last, end, period, i)
			}

			records = append(records, &billing.Record{
				Addon:    addon.GetUuid(),
				Instance: i.GetUuid(),
				Start:    last, End: end, Exec: last,
				Priority: billing.Priority_NORMAL,
				Total:    1,
			})
		}
	} else {
		end := last + period
		log.Debug("Handling Prepaid Billing", zap.Any("addon", addon.GetUuid()), zap.Int64("end", end), zap.Int64("now", time.Now().Unix()))
		for ; last <= time.Now().Unix(); end += period {
			if product.GetPeriodKind() != billing.PeriodKind_DEFAULT {
				end = utils.AlignPaymentDate(last, end, product.Period, i)
			}
			records = append(records, &billing.Record{
				Addon:    addon.GetUuid(),
				Instance: i.GetUuid(),
				Start:    last, End: end, Exec: last,
				Priority: priority,
				Total:    1,
			})
			last = end
		}
	}

	return records, last
}
