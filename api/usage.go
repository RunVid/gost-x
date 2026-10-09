package api

import (
	"github.com/gin-gonic/gin"
	"github.com/go-gost/x/internal/pineusage"
	"net/http"
)

// The private counter contract is defined in usage.yaml.
func getUsage(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, pineusage.Read())
}
