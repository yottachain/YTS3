package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/yottachain/YTCoreService/env"
)

func GetProgramVersion(g *gin.Context) {
	defer env.TracePanic("GetProgramVersion")
	//var versionID string
	versionID := "2.0.2.0"
	g.JSON(http.StatusOK, gin.H{"versionID": versionID})
}
