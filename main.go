package main

import (
	"flag"
	"os"
	"time"

	"github.com/prometheus/common/log"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTS3/conf"
	"github.com/yottachain/YTS3/routers"
)

func main() {
	log.Info(time.Now().Format("2006-01-02 15:04:05") + "strart ......")
	flag.Parse()

	var path string
	if len(os.Args) > 1 {
		if os.Args[1] != "" {
			path = os.Args[1]
		} else {
			path = "conf/yotta_config.ini"
		}

	} else {
		path = "conf/yotta_config.ini"
	}

	cfg, err := conf.CreateConfig(path)
	if err != nil {
		panic(err)
	}

	// 初始化SDK服务
	env.Console = true
	api.StartApi()

	log.Info(time.Now().Format("2006-01-02 15:04:05") + " start ......")
	router := routers.InitRouter()
	port := cfg.GetHTTPInfo("port")
	err1 := router.Run(port)
	if err1 != nil {
		panic(err1)
	}

}
