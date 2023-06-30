package pubsub

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	billingpb "github.com/slntopp/nocloud-proto/billing"
	eventpb "github.com/slntopp/nocloud-proto/events"
	i "github.com/slntopp/nocloud/pkg/instances"
	s "github.com/slntopp/nocloud/pkg/states"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type RecordsPublisher func([]*billingpb.Record)

type EventPublisher func(event *eventpb.Event)

func SetupRecordsPublisher(logger *zap.Logger, rbmq *amqp.Connection) RecordsPublisher {
	log := logger.Named("Records")

	return func(payload []*billingpb.Record) {
		ch, err := rbmq.Channel()
		if err != nil {
			log.Fatal("Failed to open a channel", zap.Error(err))
		}
		defer ch.Close()

		queue, _ := ch.QueueDeclare(
			"records",
			true, false, false, true, nil,
		)

		for _, record := range payload {
			body, err := proto.Marshal(record)
			if err != nil {
				log.Error("Error while marshalling record", zap.Error(err))
				continue
			}
			err = ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
				ContentType: "text/plain", Body: body,
			})
			if err != nil {
				log.Warn("Couldn't publish records to the queue", zap.Error(err))
			}
		}

	}
}

func SetupEventsPublisher(logger *zap.Logger, rbmq *amqp.Connection) EventPublisher {
	log := logger.Named("Events")

	return func(event *eventpb.Event) {
		ch, err := rbmq.Channel()
		if err != nil {
			log.Fatal("Failed to open a channel", zap.Error(err))
		}
		defer ch.Close()

		queue, _ := ch.QueueDeclare(
			"events",
			true, false, false, true, nil,
		)

		body, err := proto.Marshal(event)
		if err != nil {
			log.Error("Error while marshalling record", zap.Error(err))
			return
		}
		err = ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
			ContentType: "text/plain", Body: body,
		})
		if err != nil {
			log.Warn("Couldn't publish records to the queue", zap.Error(err))
		}
	}

}

func SetupSPStatesPublisher(logger *zap.Logger, rbmq *amqp.Connection) s.Pub {
	log := logger.Named("States")
	s := s.NewStatesPubSub(log, nil, rbmq)
	sch := s.Channel()
	s.TopicExchange(sch, "states")

	return s.Publisher(sch, "states", "sp")
}

func SetupInstancesStatesPublisher(logger *zap.Logger, rbmq *amqp.Connection) s.Pub {
	log := logger.Named("States")
	s := s.NewStatesPubSub(log, nil, rbmq)
	sch := s.Channel()
	s.TopicExchange(sch, "states")

	return s.Publisher(sch, "states", "instances")
}

func SetupInstancesDataPublisher(logger *zap.Logger, rbmq *amqp.Connection) i.Pub {
	log := logger.Named("Datas")
	i := i.NewPubSub(log, nil, rbmq)
	ich := i.Channel()
	i.TopicExchange(ich, "datas")

	return i.Publisher(ich, "datas", "instances")
}
