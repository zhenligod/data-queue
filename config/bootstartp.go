package config

import (
	"log"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/zhenligod/thingo/yamlconf"
)

// InitConf 初始化配置文件
func InitConf(path string) {
	conf = yamlconf.NewConf()
	err := conf.LoadConf(path + "/app.yaml")
	if err != nil {
		log.Fatalln("init config error: ", err)
	}

	AppEnv = conf.GetString("AppEnv", "production")
	switch AppEnv {
	case "local", "testing", "staging":
		AppDebug = true
	default:
		AppDebug = false
	}
}

func InitKafka() *KafkaConf {
	// 初始化kafka
	kafkaConf := &KafkaConf{}
	conf.GetStruct("KafkaClass", kafkaConf)
	return kafkaConf
}

type KafkaConf struct {
	Ip    string
	Port  string
	Topic string
}

type EsConf struct {
	IP       string
	Port     string
	Index    string
	Username string
	Passwd   string
	Es       *elasticsearch.Client
}

func InitEs() *EsConf {
	// 初始化es
	esConf := &EsConf{}
	conf.GetStruct("EsClass", esConf)
	return esConf
}
