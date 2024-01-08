package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	client  sarama.SyncProducer
	MsgChan chan *sarama.ProducerMessage
)

// 初始化全局的Kafka Client
func InitKafka(address []string, chansize int64) (err error) {
	// 生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	fmt.Println("config producer success")

	// 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("producer closed, err :", err)
		return
	}
	fmt.Printf("connect scuucess client:%v,err:%v", client, err)
	MsgChan = make(chan *sarama.ProducerMessage, chansize)
	go sendMsg()
	return
}

// 从通道中读取msg 发送给kafka
func sendMsg() {
	fmt.Println("sendmsg function used!")
	for {
		select {
		case msg := <-MsgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed, err:", err)
				return
			}
			logrus.Infof("send msg to kafka success pid:%v offset:%v", pid, offset)
		}
	}
}
