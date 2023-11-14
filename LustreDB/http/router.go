package main

import "github.com/gin-gonic/gin"

func InitializeRoutes(r *gin.Engine) {
	apiGroup := r.Group("/")
	{
		api := new(CommonApi)
		apiGroup.POST("put", api.PutApi)
		apiGroup.POST("get", api.GetApi)
		apiGroup.POST("delete", api.DelApi)
		apiGroup.GET("list", api.ListApi)
	}
	_ = r.Group("/batch")
	{
		// TODO:事物接口
	}

}
