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
	"context"
	"github.com/go-redis/redis/v8"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/slntopp/nocloud-driver-virtual/internal/actions"
	"github.com/slntopp/nocloud-driver-virtual/internal/server"
	"github.com/slntopp/nocloud-proto/ansible"
	"github.com/slntopp/nocloud-proto/drivers/instance/vanilla"
	iconnect "github.com/slntopp/nocloud-proto/instances/instancesconnect"
	"github.com/slntopp/nocloud/pkg/nocloud"
	"github.com/slntopp/nocloud/pkg/nocloud/auth"
	grpc_server "github.com/slntopp/nocloud/pkg/nocloud/grpc"
	"github.com/slntopp/nocloud/pkg/nocloud/rabbitmq"
	"github.com/slntopp/nocloud/pkg/nocloud/schema"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"net/http"
)

var (
	port     string
	type_key string

	SIGNING_KEY []byte

	log *zap.Logger

	RabbitMQConn string

	ansibleHost string

	redisHost string

	instancesHost string
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

	viper.SetDefault("ANSIBLE_HOST", "ansible:8000")
	ansibleHost = viper.GetString("ANSIBLE_HOST")

	viper.SetDefault("SIGNING_KEY", "seeeecreet")
	SIGNING_KEY = []byte(viper.GetString("SIGNING_KEY"))

	viper.SetDefault("REDIS_HOST", "redis:6379")
	redisHost = viper.GetString("REDIS_HOST")

	viper.SetDefault("INSTANCES_HOST", "instances:8000")
	instancesHost = viper.GetString("INSTANCES_HOST")
}

// dev
func main() {
	defer func() {
		_ = log.Sync()
	}()

	log.Info("Dialing RabbitMQ", zap.String("url", RabbitMQConn))
	rbmq, err := amqp.Dial(RabbitMQConn)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
	}
	defer rbmq.Close()
	rabbitmq.FatalOnConnectionClose(log, rbmq)

	log.Info("Connecting redis", zap.String("url", redisHost))
	rdb := redis.NewClient(&redis.Options{
		Addr: redisHost,
		DB:   0, // use default DB
	})
	if resp := rdb.Ping(context.Background()); resp.Err() != nil {
		log.Fatal("Failed to establish redis connection", zap.Error(resp.Err()))
	}
	log.Info("RedisDB connection established")

	s := grpc.NewServer()
	srv := server.NewVirtualDriver(log, rbmq, rdb, SIGNING_KEY, type_key)

	if ansibleHost != "" {
		log.Info("Ansible host", zap.String("Host", ansibleHost))
		dial, err := grpc.Dial(ansibleHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			ansibleClient := ansible.NewAnsibleServiceClient(dial)
			token, _ := auth.MakeToken(schema.ROOT_ACCOUNT_KEY)
			ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "bearer "+token)

			srv.SetAnsibleClient(ctx, ansibleClient)
		} else {
			log.Fatal("Failed to setup ansible connection", zap.Error(err))
		}
	}

	if instancesHost != "" {
		actions.SetInstancesClient(iconnect.NewInstancesServiceClient(http.DefaultClient, instancesHost))
	}

	vanilla.RegisterDriverServiceServer(s, srv)

	grpc_server.ServeGRPC(log, s, port)
}
