package main

import (
	"github.com/yottachain/YTCoreService/api/backend"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTS3/routers"
)

func main() {
	env.YTS3.AddStart(backend.StartS3)
	env.YTS3.AddStart(routers.StartServer)
	env.YTS3.AddStop(backend.StopS3)
	env.LaunchYTS3()
}
