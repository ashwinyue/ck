# CK - Kratos 微服务项目

这是一个基于 [Kratos](https://go-kratos.dev/) 框架构建的微服务项目，集成了 MySQL、Redis 和 RocketMQ。

## 功能特性

- ✅ **MySQL**: 使用 GORM 作为 ORM，支持自动迁移和连接池配置
- ✅ **Redis**: 集成 Redis 客户端，支持缓存和分布式锁
- ✅ **RocketMQ**: 集成消息队列，支持生产者和消费者
- ✅ **依赖注入**: 使用 Wire 进行依赖注入
- ✅ **配置管理**: 支持 YAML 配置文件和环境变量
- ✅ **日志系统**: 集成结构化日志
- ✅ **健康检查**: 内置健康检查端点

## 项目结构

```
.
├── api/                    # API 定义 (protobuf)
├── cmd/ck/                 # 应用程序入口
├── configs/                # 配置文件
├── internal/               # 内部代码
│   ├── biz/               # 业务逻辑层
│   ├── conf/              # 配置结构体
│   ├── data/              # 数据访问层
│   ├── server/            # 服务器配置
│   └── service/           # 服务层
├── third_party/           # 第三方 proto 文件
├── docker-compose.yml     # Docker 编排文件
├── broker.conf           # RocketMQ Broker 配置
└── README.md
```

## 快速开始

### 1. 启动依赖服务

使用 Docker Compose 启动 MySQL、Redis 和 RocketMQ：

```bash
docker-compose up -d
```

等待所有服务启动完成后，可以通过以下地址访问：

- **MySQL**: `localhost:3306` (用户名: `ck`, 密码: `ck123456`)
- **Redis**: `localhost:6379`
- **RocketMQ Console**: `http://localhost:8080`
- **RocketMQ NameServer**: `localhost:9876`

### 2. 配置应用

编辑 `configs/config.yaml` 文件，确保数据库连接信息正确：

```yaml
data:
  database:
    source: "ck:ck123456@tcp(localhost:3306)/ck?charset=utf8mb4&parseTime=True&loc=Local"
    max_idle_conns: 10
    max_open_conns: 100
    conn_max_lifetime: 3600s
  redis:
    addr: "localhost:6379"
    password: ""
    db: 0
    dial_timeout: 5s
    read_timeout: 3s
    write_timeout: 3s
    pool_size: 10
    min_idle_conns: 5
  rocketmq:
    name_server: "localhost:9876"
    group_name: "ck_group"
    retry_times: 3
    timeout: 3s
```

### 3. 构建和运行

```bash
# 安装依赖
go mod tidy

# 生成代码
go generate ./...

# 构建项目
go build -o ./bin/ ./...

# 运行应用
./bin/ck -conf ./configs
```

应用启动后：
- HTTP 服务: `http://localhost:8000`
- gRPC 服务: `localhost:9000`

### 4. 测试 API

创建一个 Greeter：

```bash
curl -X POST http://localhost:8000/helloworld/greeter \
  -H "Content-Type: application/json" \
  -d '{"name": "world"}'
```

获取 Greeter：

```bash
curl http://localhost:8000/helloworld/greeter/world
```

## 集成说明

### MySQL 集成

- 使用 GORM 作为 ORM
- 支持自动表迁移
- 配置了连接池参数
- 示例模型：`GreeterModel`

### Redis 集成

- 用作缓存层
- 支持数据序列化/反序列化
- 配置了连接超时和连接池
- 缓存策略：查询时先查缓存，缓存未命中时查数据库并更新缓存

### RocketMQ 集成

- 集成了生产者和消费者
- 支持同步/异步消息发送
- 自动处理消息重试
- 示例：在数据变更时发送事件消息

## 开发指南

### 添加新的 API

1. 在 `api/` 目录下定义 protobuf 文件
2. 运行 `make api` 生成代码
3. 在 `internal/service/` 中实现服务逻辑
4. 在 `internal/biz/` 中实现业务逻辑
5. 在 `internal/data/` 中实现数据访问

### 数据库迁移

项目使用 GORM 的 AutoMigrate 功能，在应用启动时自动创建/更新表结构。

### 消息队列使用

```go
// 发送消息
msg := &primitive.Message{
    Topic: "your_topic",
    Body:  []byte("your message"),
}
result, err := producer.SendSync(ctx, msg)

// 消费消息（在 NewRocketMQConsumer 中配置）
consumer.Subscribe("your_topic", consumer.MessageSelector{}, 
    func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
        // 处理消息
        return consumer.ConsumeSuccess, nil
    })
```

## 监控和运维

- 应用日志：结构化日志输出
- 健康检查：`/health` 端点
- 指标监控：可集成 Prometheus
- RocketMQ 控制台：`http://localhost:8080`

## 部署

### Docker 部署

```bash
# 构建镜像
docker build -t ck:latest .

# 运行容器
docker run -d --name ck \
  --network host \
  -v $(pwd)/configs:/app/configs \
  ck:latest
```

### Kubernetes 部署

可以使用 Helm Chart 或 Kustomize 进行 Kubernetes 部署。

## 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查 MySQL 服务是否启动
   - 验证连接字符串和凭据
   - 确认网络连通性

2. **Redis 连接失败**
   - 检查 Redis 服务状态
   - 验证地址和端口配置

3. **RocketMQ 连接失败**
   - 确认 NameServer 地址正确
   - 检查 Broker 是否正常启动
   - 查看 RocketMQ 控制台状态

### 日志查看

```bash
# 查看应用日志
tail -f /var/log/ck/app.log

# 查看 Docker 容器日志
docker logs -f ck

# 查看依赖服务日志
docker-compose logs -f mysql redis rocketmq-namesrv rocketmq-broker
```

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License

