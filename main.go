package main

import (
	"fmt"
	"time"

	kafkaConf "ekreke.github.com/logagent/kafkaConf"
	tailfile "ekreke.github.com/logagent/tailfile"

	"github.com/Shopify/sarama"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

// 日志收集客户端
// 类似：filebeat
// 收集指定目录下的日志文件，发送到kafka中

// 往kafka发数据
//使用tail读取日志文件

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

func run() (err error) {
	for {
		// 循环读数据
		line, ok := <-tailfile.TailObj.Lines // chan tail.Line
		if !ok {
			logrus.Warn("tail file close reopen, filename:%s\n", tailfile.TailObj.Filename)
			time.Sleep(time.Second)
			continue
		}
		//利用通道将同步的代码改为异步
		//把读出来的一行日志包装成kafka里面的message类型，丢到通道中
		fmt.Println(line.Text)
		msg := &sarama.ProducerMessage{}
		msg.Topic = "web_log"
		msg.Value = sarama.StringEncoder(line.Text)
		// 放到channel中
		kafkaConf.MsgChan <- msg
	}
}

func main() {
	var configObj = new(Config)
	// cfg, err := ini.Load("./conf/config.ini")
	// if err != nil {
	// 	logrus.Error("load config failed,err :%v", err)
	// 	return
	// }
	// kafkaAddr := cfg.Section("kafka").Key("address").String()
	// fmt.Println(kafkaAddr)
	err := ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Error("load config failed,err:%v", err)
		return
	}
	fmt.Println("map config ended...")
	fmt.Println("start init kafka...")
	// 初始化连接kafka
	err = kafkaConf.InitKafka([]string{configObj.Address}, configObj.KafkaConfig.ChanSize)
	fmt.Println(err)
	if err != nil {
		logrus.Error("init kafka failed err:%v", err)
	}
	logrus.Info("init kafka success")
	// 根据配置中的日志路径初始化tail
	err = tailfile.Init(configObj.CollectConfig.LogFilePath)
	if err != nil {
		logrus.Error("init tailfile failed , err:%v", err)
		return
	}
	logrus.Info("init tailfile success!")
	// 把日志通过sarama发往kafka
	// TailObj --> log -->Client -- > kafka
	err = run()
	if err != nil {
		logrus.Info("run failed... err:%v", err)
		return
	}

}
