package data

import (
	"context"
	"time"

	"ck/internal/conf"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/google/wire"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewDB, NewRedis, NewRocketMQProducer, NewRocketMQConsumer, NewGreeterRepo, NewETLTaskRepo)

// Data .
type Data struct {
	db         *gorm.DB
	rdb        *redis.Client
	mqProducer rocketmq.Producer
	mqConsumer rocketmq.PushConsumer
}

// NewData .
func NewData(c *conf.Data, logger log.Logger, db *gorm.DB, rdb *redis.Client, mqProducer rocketmq.Producer, mqConsumer rocketmq.PushConsumer) (*Data, func(), error) {
	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
		rdb.Close()
		mqProducer.Shutdown()
		mqConsumer.Shutdown()
	}
	return &Data{
		db:         db,
		rdb:        rdb,
		mqProducer: mqProducer,
		mqConsumer: mqConsumer,
	}, cleanup, nil
}

// NewDB .
func NewDB(c *conf.Data) *gorm.DB {
	db, err := gorm.Open(mysql.Open(c.Database.Source), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic("failed to connect database")
	}

	// 获取通用数据库对象 sql.DB ，然后使用其提供的功能
	sqlDB, err := db.DB()
	if err != nil {
		panic("failed to get sql.DB")
	}

	// SetMaxIdleConns 用于设置连接池中空闲连接的最大数量
	sqlDB.SetMaxIdleConns(int(c.Database.MaxIdleConns))

	// SetMaxOpenConns 设置打开数据库连接的最大数量
	sqlDB.SetMaxOpenConns(int(c.Database.MaxOpenConns))

	// SetConnMaxLifetime 设置了连接可复用的最大时间
	sqlDB.SetConnMaxLifetime(c.Database.ConnMaxLifetime.AsDuration())

	return db
}

// NewRedis .
func NewRedis(c *conf.Data) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         c.Redis.Addr,
		Password:     c.Redis.Password,
		DB:           int(c.Redis.Db),
		DialTimeout:  c.Redis.DialTimeout.AsDuration(),
		ReadTimeout:  c.Redis.ReadTimeout.AsDuration(),
		WriteTimeout: c.Redis.WriteTimeout.AsDuration(),
		PoolSize:     int(c.Redis.PoolSize),
		MinIdleConns: int(c.Redis.MinIdleConns),
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic("failed to connect redis: " + err.Error())
	}

	return rdb
}

// NewRocketMQProducer .
func NewRocketMQProducer(c *conf.Data) rocketmq.Producer {
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{c.Rocketmq.NameServer}),
		producer.WithRetry(int(c.Rocketmq.RetryTimes)),
		producer.WithGroupName(c.Rocketmq.GroupName),
	)
	if err != nil {
		panic("failed to create rocketmq producer: " + err.Error())
	}

	err = p.Start()
	if err != nil {
		panic("failed to start rocketmq producer: " + err.Error())
	}

	return p
}

// NewRocketMQConsumer .
func NewRocketMQConsumer(c *conf.Data) rocketmq.PushConsumer {
	consumerGroup := c.Rocketmq.GroupName + "_consumer"
	consumerClient, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{c.Rocketmq.NameServer}),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName(consumerGroup),
	)
	if err != nil {
		panic("failed to create rocketmq consumer: " + err.Error())
	}

	// 注册消息处理函数
	err = consumerClient.Subscribe("test_topic", consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			log.Infof("Received message: %s", string(msg.Body))
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		panic("failed to subscribe rocketmq topic: " + err.Error())
	}

	err = consumerClient.Start()
	if err != nil {
		panic("failed to start rocketmq consumer: " + err.Error())
	}

	return consumerClient
}
