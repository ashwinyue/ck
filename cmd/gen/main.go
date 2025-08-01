package main

import (
	"gorm.io/driver/mysql"
	"gorm.io/gen"
	"gorm.io/gorm"
)

func main() {
	// 连接数据库
	db, err := gorm.Open(mysql.Open("root:root@tcp(127.0.0.1:3306)/ck?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// 创建生成器
	g := gen.NewGenerator(gen.Config{
		OutPath:       "./internal/data/query",                                            // 生成代码的输出目录
		Mode:          gen.WithoutContext | gen.WithDefaultQuery | gen.WithQueryInterface, // 生成模式
		FieldNullable: true,                                                               // 字段可为空
	})

	// 使用数据库连接
	g.UseDB(db)

	// 生成ODS层订单模型
	g.ApplyBasic(
		// ODS层订单表
		g.GenerateModel("ods_orders"),
	)

	// 执行生成
	g.Execute()
}
