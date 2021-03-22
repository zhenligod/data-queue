package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zhenligod/data-queue/config"
	"github.com/zhenligod/data-queue/logic"

	"github.com/zhenligod/thingo/logger"
	_ "go.uber.org/automaxprocs"
)

var (
	port      int
	logDir    string
	configDir string

	wait time.Duration // 平滑重启的等待时间1s or 1m
)

func init() {
	flag.IntVar(&port, "port", 1339, "app listen port")
	flag.StringVar(&logDir, "log_dir", "./logs", "log dir")
	flag.StringVar(&configDir, "config_dir", "./", "config dir")
	flag.DurationVar(&wait, "graceful-timeout", 3*time.Second, "the server gracefully reload. eg: 15s or 1m")
	flag.Parse()

	// 日志文件设置
	logger.SetLogDir(logDir)
	logger.SetLogFile("data-queue.log")
	logger.MaxSize(500)

	// 由于app/extensions/logger基于thingo/logger又包装了一层，所以这里是3
	logger.InitLogger(3)

	// 初始化配置文件
	config.InitConf(configDir)

}

func main() {
	kafkaConf := config.InitKafka()
	configKafka := logic.KafkaConf{
		IP:    kafkaConf.Ip,
		Port:  kafkaConf.Port,
		Topic: kafkaConf.Topic,
	}

	kafkaSvc := &logic.KafkaLogic{
		BaseLogic: logic.BaseLogic{},
	}

	log.Println("server pid: ", os.Getppid())
	log.Println("kafka conf: ", kafkaConf)

	go func() {
		defer logger.Recover()
		kafkaSvc.Customer(configKafka)
	}()

	// 平滑重启
	ch := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// recivie signal to exit main goroutine
	// window signal
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, syscall.SIGHUP)

	// linux signal if you use linux on production,please use this code.
	// signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2, os.Interrupt, syscall.SIGHUP)

	// Block until we receive our signal.
	sig := <-ch

	log.Println("exit signal: ", sig.String())
	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	<-ctx.Done()

	log.Println("server shutting down")
}
