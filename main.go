/*
Copyright © 2023 Nikita Ivanovski info@slnt-opp.xyz

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"net"

	billingpb "github.com/slntopp/nocloud-proto/billing"

	"github.com/slntopp/nocloud-driver-virtual/internal/server"
	"github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	"github.com/slntopp/nocloud/pkg/nocloud"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	port     string
	type_key string

	log          *zap.Logger
	statesHost   string
	RabbitMQConn string
)

func init() {
	viper.AutomaticEnv()
	log = nocloud.NewLogger()

	viper.SetDefault("PORT", "8080")
	port = viper.GetString("PORT")

	viper.SetDefault("DRIVER_TYPE_KEY", "virtual")
	type_key = viper.GetString("DRIVER_TYPE_KEY")

	viper.SetDefault("RABBITMQ_CONN", "amqp://nocloud:secret@rabbitmq:5672/")
	RabbitMQConn = viper.GetString("RABBITMQ_CONN")
}

func main() {
	defer func() {
		_ = log.Sync()
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Fatal("Failed to listen", zap.String("address", port), zap.Error(err))
	}

	log.Info("Dialing RabbitMQ", zap.String("url", RabbitMQConn))
	rbmq, err := amqp.Dial(RabbitMQConn)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
	}
	defer rbmq.Close()

	s := grpc.NewServer()
	srv := server.NewVirtualDriver(log, type_key)
	srv.HandlePublishRecords = SetupRecordsPublisher(rbmq)

	vanilla.RegisterDriverServiceServer(s, srv)

	log.Info(fmt.Sprintf("Serving gRPC on 0.0.0.0:%v", port))
	log.Fatal("Failed to serve gRPC", zap.Error(s.Serve(lis)))
}

func SetupRecordsPublisher(rbmq *amqp.Connection) server.RecordsPublisher {
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
			ch.Publish("", queue.Name, false, false, amqp.Publishing{
				ContentType: "text/plain", Body: body,
			})
		}

	}
}
