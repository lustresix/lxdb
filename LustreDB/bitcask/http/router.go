package main

import "github.com/gin-gonic/gin"

func InitializeRoutes(r *gin.Engine) {
	r.POST("put", PutApi)
	r.POST("get", GetApi)
}
