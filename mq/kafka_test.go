package mq

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/ljjgs/pkg/logger"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

var (
	hosts = []string{"127.0.0.1:9200"}
	topoc = "test"
)

type Msg struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`
}

func ProduceTest(t *testing.B) {
	err := InitSyncKafkaProducer(DefaultKafkaSyncProducer, hosts, nil)

	if err == nil {
		fmt.Println("InitAsyncKafkaProducer error", err)
	}

	msg := &Msg{
		ID:       100,
		Name:     "name",
		CreateAt: 0,
	}
	msgBody, _ := json.Marshal(msg)
	GetKafkaSyncProducer(DefaultKafkaAsyncProducer).Send(&sarama.ProducerMessage{
		Topic: topoc,
		Value: KafkaMsgValueEncoder(msgBody),
	})

	if err != nil {
		fmt.Println("Send msg error", err)
	} else {
		fmt.Println("Send msg success")
	}
	//异步提交需要等待
	time.Sleep(3 * time.Second)
}

func ConsumerTest(t *testing.B) {
	_, err := StartKafkaConsumer(hosts, []string{topoc}, "test-group", nil, msgHandler)
	if err != nil {
		fmt.Println(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	select {
	case s := <-signals:
		KafkaStdLogger.Println("kafka test receive system signal:", s)
		return
	}
}

func msgHandler(message *sarama.ConsumerMessage) (bool, error) {
	fmt.Println("消费消息:", "topic:", message.Topic, "Partition:", message.Partition, "Offset:", message.Offset, "value:", string(message.Value))
	msg := Msg{}
	err := json.Unmarshal(message.Value, &msg)
	if err != nil {
		//解析不了的消息怎么处理？
		logger.Error("Unmarshal error", zap.Error(err))
		return true, nil
	}
	fmt.Println("msg : ", msg)
	return true, nil
}
