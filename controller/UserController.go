package controller

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/common/log"
	"github.com/yottachain/YTCoreService/api"
)

//Register 用户注册
func Register(g *gin.Context) {

	var count int = 0

	userName := g.PostForm("userName")

	privateKey := g.PostForm("privateKey")

	log.Info("userName : " + userName)
	log.Info("privateKey : " + privateKey)

	if count == 0 {
		c, err := api.NewClient(userName, privateKey)
		if err != nil {

		} else {
			count = 1
			log.Info("User Register Success,UserName:" + "testnodeyyy1")
			fmt.Println("UserID:", c.UserId)
		}
	}

}
