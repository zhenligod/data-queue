// Package kafka provides ...
package logic

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/zhenligod/thingo/logger"

	"github.com/Shopify/sarama"
)

// KafkaLogic
type KafkaLogic struct {
	BaseLogic
}

// KafkaConf kafka 配置
type KafkaConf struct {
	IP    string
	Port  string
	Topic string
}

type ClassStaff struct {
	CompanyId     string      `json:"company_id"`
	ClassId       string      `json:"class_id"`
	StaffId       string      `json:"staff_id"`
	LearnTime     int         `json:"learn_time"`
	LastCourseId  string      `json:"last_course_id"`
	Procedure     int         `json:"procedure"`
	Status        int         `json:"status"`
	IsRemoved     bool        `json:"is_removed"`
	IsDeleted     bool        `json:"is_deleted"`
	SucceededAt   interface{} `json:"succeeded_at"`
	StartedAt     interface{} `json:"started_at"`
	CreatedAt     interface{} `json:"created_at"`
	UpdatedAt     interface{} `json:"updated_at"`
	SourceType    string      `json:"source_type"`
	TeamId        string      `json:"team_id"`
	AppSourceType string      `json:"app_source_type"`
	RoadmapId     string      `json:"roadmap_id"`
	ClassType     string      `json:"class_type"`
	CategoryId    string      `json:"category_id"`
	IsRequired    bool        `json:"is_required"`
}

func (k *KafkaLogic) Customer(conf KafkaConf, esConf EsConf) error {
	address := strings.Join([]string{
		conf.IP,
		conf.Port,
	}, ":")
	// 生成消费者 实例
	consumer, err := sarama.NewConsumer([]string{address}, nil)
	if err != nil {
		logger.Info("server close error", map[string]interface{}{
			"trace_error": err.Error(),
		})
		return err
	}
	// 拿到 对应主题下所有分区
	partitionList, err := consumer.Partitions(conf.Topic)
	if err != nil {
		logger.Info("server close error", map[string]interface{}{
			"trace_error": err.Error(),
		})
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	// 遍历所有分区
	for partition := range partitionList {
		//消费者 消费 对应主题的 具体 分区 指定 主题 分区 offset  return 对应分区的对象
		pc, err := consumer.ConsumePartition(conf.Topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			logger.Info("server close error", map[string]interface{}{
				"trace_error": err.Error(),
			})
			return err
		}

		// 运行完毕记得关闭
		defer pc.AsyncClose()

		// 去出对应的 消息
		// 通过异步 拿到 消息
		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				UpdateEsClassStaff(msg.Value)
			}
		}(pc)
	}
	wg.Wait()
	return nil
}

func UpdateEsClassStaff(Value []byte) error {
	// fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v", Partition, Offset, Key, Value)
	ClassStaff := &ClassStaff{}
	json.Unmarshal(Value, ClassStaff)
	docID := strings.Join([]string{
		ClassStaff.CompanyId,
		ClassStaff.ClassId,
		ClassStaff.StaffId,
	}, "-")

	esSvc := EsLogic{
		BaseLogic: BaseLogic{},
	}

	res, err := esSvc.UpdateDoc(docID, string(Value[:]))
	defer res.Body.Close()
	if err != nil {
		logger.Info("create doc error", map[string]interface{}{
			"trace_error": err.Error(),
		})
	}
	return nil
}
