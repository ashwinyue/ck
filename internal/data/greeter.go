package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ck/internal/biz"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/go-kratos/kratos/v2/log"
)

// GreeterModel 数据库模型
type GreeterModel struct {
	ID        int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Hello     string    `gorm:"size:255;not null" json:"hello"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName 指定表名
func (GreeterModel) TableName() string {
	return "greeters"
}

type greeterRepo struct {
	data *Data
	log  *log.Helper
}

// NewGreeterRepo .
func NewGreeterRepo(data *Data, logger log.Logger) biz.GreeterRepo {
	// 自动迁移数据库表
	data.db.AutoMigrate(&GreeterModel{})

	return &greeterRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

func (r *greeterRepo) Save(ctx context.Context, g *biz.Greeter) (*biz.Greeter, error) {
	// 保存到数据库
	model := &GreeterModel{
		Hello: g.Hello,
	}

	if err := r.data.db.WithContext(ctx).Create(model).Error; err != nil {
		r.log.Errorf("Failed to save greeter to database: %v", err)
		return nil, err
	}

	// 缓存到Redis
	cacheKey := fmt.Sprintf("greeter:%d", model.ID)
	cacheData, _ := json.Marshal(model)
	if err := r.data.rdb.Set(ctx, cacheKey, cacheData, 10*time.Minute).Err(); err != nil {
		r.log.Warnf("Failed to cache greeter to redis: %v", err)
	}

	// 发送消息到RocketMQ
	msgData, _ := json.Marshal(map[string]interface{}{
		"action": "create",
		"id":     model.ID,
		"hello":  model.Hello,
		"time":   time.Now(),
	})

	msg := &primitive.Message{
		Topic: "greeter_events",
		Body:  msgData,
	}

	if _, err := r.data.mqProducer.SendSync(ctx, msg); err != nil {
		r.log.Warnf("Failed to send message to rocketmq: %v", err)
	}

	return &biz.Greeter{
		Hello: model.Hello,
	}, nil
}

func (r *greeterRepo) Update(ctx context.Context, g *biz.Greeter) (*biz.Greeter, error) {
	// 需要先查找现有记录获取ID
	var existingModel GreeterModel
	if err := r.data.db.WithContext(ctx).Where("hello = ?", g.Hello).First(&existingModel).Error; err != nil {
		r.log.Errorf("Failed to find existing greeter for update: %v", err)
		return nil, err
	}

	model := &GreeterModel{
		ID:    existingModel.ID,
		Hello: g.Hello,
	}

	if err := r.data.db.WithContext(ctx).Save(model).Error; err != nil {
		r.log.Errorf("Failed to update greeter in database: %v", err)
		return nil, err
	}

	// 更新Redis缓存
	cacheKey := fmt.Sprintf("greeter:%d", model.ID)
	cacheData, _ := json.Marshal(model)
	if err := r.data.rdb.Set(ctx, cacheKey, cacheData, 10*time.Minute).Err(); err != nil {
		r.log.Warnf("Failed to update cache in redis: %v", err)
	}

	// 发送更新消息到RocketMQ
	msgData, _ := json.Marshal(map[string]interface{}{
		"action": "update",
		"id":     model.ID,
		"hello":  model.Hello,
		"time":   time.Now(),
	})

	msg := &primitive.Message{
		Topic: "greeter_events",
		Body:  msgData,
	}

	if _, err := r.data.mqProducer.SendSync(ctx, msg); err != nil {
		r.log.Warnf("Failed to send update message to rocketmq: %v", err)
	}

	return g, nil
}

func (r *greeterRepo) FindByID(ctx context.Context, id int64) (*biz.Greeter, error) {
	// 先从Redis缓存中查找
	cacheKey := fmt.Sprintf("greeter:%d", id)
	cacheData, err := r.data.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var model GreeterModel
		if json.Unmarshal([]byte(cacheData), &model) == nil {
			r.log.Infof("Found greeter in cache: %d", id)
			return &biz.Greeter{
				Hello: model.Hello,
			}, nil
		}
	}

	// 从数据库查找
	var model GreeterModel
	if err := r.data.db.WithContext(ctx).First(&model, id).Error; err != nil {
		r.log.Errorf("Failed to find greeter by id %d: %v", id, err)
		return nil, err
	}

	// 缓存到Redis
	cacheDataBytes, _ := json.Marshal(model)
	if err := r.data.rdb.Set(ctx, cacheKey, string(cacheDataBytes), 10*time.Minute).Err(); err != nil {
		r.log.Warnf("Failed to cache greeter to redis: %v", err)
	}

	return &biz.Greeter{
		Hello: model.Hello,
	}, nil
}

func (r *greeterRepo) ListByHello(ctx context.Context, hello string) ([]*biz.Greeter, error) {
	// 先尝试从Redis获取
	cacheKey := fmt.Sprintf("greeter_list:%s", hello)
	cacheData, err := r.data.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var greeters []*biz.Greeter
		if json.Unmarshal([]byte(cacheData), &greeters) == nil {
			r.log.Infof("Found greeter list in cache for hello: %s", hello)
			return greeters, nil
		}
	}

	// 从数据库查询
	var models []GreeterModel
	if err := r.data.db.WithContext(ctx).Where("hello LIKE ?", "%"+hello+"%").Find(&models).Error; err != nil {
		r.log.Errorf("Failed to list greeters by hello %s: %v", hello, err)
		return nil, err
	}

	// 转换为业务对象
	greeters := make([]*biz.Greeter, 0, len(models))
	for _, model := range models {
		greeters = append(greeters, &biz.Greeter{
			Hello: model.Hello,
		})
	}

	// 缓存结果
	cacheDataBytes, _ := json.Marshal(greeters)
	if err := r.data.rdb.Set(ctx, cacheKey, string(cacheDataBytes), 5*time.Minute).Err(); err != nil {
		r.log.Warnf("Failed to cache greeter list to redis: %v", err)
	}

	return greeters, nil
}

func (r *greeterRepo) ListAll(ctx context.Context) ([]*biz.Greeter, error) {
	// 先尝试从Redis获取
	cacheKey := "greeter_list_all"
	cacheData, err := r.data.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var greeters []*biz.Greeter
		if json.Unmarshal([]byte(cacheData), &greeters) == nil {
			r.log.Info("Found all greeters in cache")
			return greeters, nil
		}
	}

	// 从数据库查询
	var models []GreeterModel
	if err := r.data.db.WithContext(ctx).Find(&models).Error; err != nil {
		r.log.Errorf("Failed to list all greeters: %v", err)
		return nil, err
	}

	// 转换为业务对象
	greeters := make([]*biz.Greeter, 0, len(models))
	for _, model := range models {
		greeters = append(greeters, &biz.Greeter{
			Hello: model.Hello,
		})
	}

	// 缓存结果
	cacheDataBytes, _ := json.Marshal(greeters)
	if err := r.data.rdb.Set(ctx, cacheKey, string(cacheDataBytes), 5*time.Minute).Err(); err != nil {
		r.log.Warnf("Failed to cache all greeters to redis: %v", err)
	}

	return greeters, nil
}
