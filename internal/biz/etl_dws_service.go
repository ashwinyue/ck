package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/go-kratos/kratos/v2/log"
)

// ETLDWSService DWD到DWS的ETL服务
type ETLDWSService struct {
	etlTaskRepo   ETLTaskRepo
	orderDataRepo OrderDataRepo
	transformer   *OrderTransformer
	mqProducer    rocketmq.Producer
	log           *log.Helper
}

// NewETLDWSService 创建DWD到DWS的ETL服务
func NewETLDWSService(etlTaskRepo ETLTaskRepo, orderDataRepo OrderDataRepo, mqProducer rocketmq.Producer, logger log.Logger) *ETLDWSService {
	return &ETLDWSService{
		etlTaskRepo:   etlTaskRepo,
		orderDataRepo: orderDataRepo,
		transformer:   &OrderTransformer{},
		mqProducer:    mqProducer,
		log:           log.NewHelper(logger),
	}
}

// CreateDWDToDWSTask 创建DWD到DWS的ETL任务
func (s *ETLDWSService) CreateDWDToDWSTask(ctx context.Context, batchSize int) (*ETLTask, error) {
	// 创建任务配置
	config := map[string]interface{}{
		"batch_size": batchSize,
		"source":     "dwd_orders",
		"target":     "dws_orders",
	}
	configJSON, _ := json.Marshal(config)

	// 创建ETL任务
	task := &ETLTask{
		Name:         "DWD_TO_DWS_ORDER_PROCESSING",
		Description:  "Process DWD orders to DWS layer with field extraction",
		Config:       string(configJSON),
		Status:       ETLTaskStatusPending,
		CurrentStage: ETLStageODS, // 开始阶段
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	createdTask, err := s.etlTaskRepo.CreateTask(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to create DWD to DWS task: %w", err)
	}

	s.log.Infof("Created DWD to DWS ETL task with ID: %d", createdTask.ID)
	return createdTask, nil
}

// ProcessDWDToDWS 处理DWD到DWS的数据转换
func (s *ETLDWSService) ProcessDWDToDWS(ctx context.Context, taskID int64, batchSize int) error {
	s.log.Infof("Starting DWD to DWS processing for task %d", taskID)

	// 获取任务
	task, err := s.etlTaskRepo.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task %d: %w", taskID, err)
	}

	// 更新任务状态为运行中
	task.Status = ETLTaskStatusRunning
	task.CurrentStage = ETLStageDWS // DWS处理阶段
	task.UpdatedAt = time.Now()
	_, err = s.etlTaskRepo.UpdateTask(ctx, task)
	if err != nil {
		s.log.Errorf("Failed to update task status: %v", err)
	}

	// 创建阶段执行记录
	stageExecution := &ETLStageExecution{
		TaskID:    taskID,
		Stage:     ETLStageDWS,
		Status:    string(ETLTaskStatusRunning),
		StartedAt: time.Now(),
	}
	_, err = s.etlTaskRepo.CreateStageExecution(ctx, stageExecution)
	if err != nil {
		s.log.Errorf("Failed to create stage execution: %v", err)
	}

	// 开始处理数据
	cursor := task.Cursor // 从任务保存的游标位置开始
	totalProcessed := 0

	for {
		// 从DWD层获取数据
		dwdOrders, nextCursor, err := s.orderDataRepo.GetDWDOrdersByCursor(ctx, cursor, batchSize)
		if err != nil {
			s.updateTaskError(ctx, task, stageExecution, fmt.Errorf("failed to get DWD orders: %w", err))
			return err
		}

		// 如果没有更多数据，结束处理
		if len(dwdOrders) == 0 {
			s.log.Info("No more DWD orders to process")
			break
		}

		// 转换数据
		dwsOrders := make([]*DWSOrder, 0, len(dwdOrders))
		for _, dwdOrder := range dwdOrders {
			dwsOrder, err := s.transformer.TransformDWDToDWS(dwdOrder)
			if err != nil {
				s.log.Warnf("Failed to transform DWD order %s: %v", dwdOrder.OrderID, err)
				continue
			}
			dwsOrders = append(dwsOrders, dwsOrder)
		}

		// 批量插入到DWS层
		if len(dwsOrders) > 0 {
			err = s.orderDataRepo.BatchInsertDWSOrders(ctx, dwsOrders)
			if err != nil {
				s.updateTaskError(ctx, task, stageExecution, fmt.Errorf("failed to insert DWS orders: %w", err))
				return err
			}
			totalProcessed += len(dwsOrders)
			s.log.Infof("Processed batch: %d DWS orders, total: %d", len(dwsOrders), totalProcessed)

			// 推送到下游系统
			for _, dwsOrder := range dwsOrders {
				err = s.pushOrderToDownstream(ctx, dwsOrder)
				if err != nil {
					s.log.Errorf("Failed to push order %s to downstream: %v", dwsOrder.OrderID, err)
					// 更新推送状态为失败
					s.UpdateOrderPushStatus(ctx, dwsOrder.OrderID, "failed")
				} else {
					// 更新推送状态为成功
					s.UpdateOrderPushStatus(ctx, dwsOrder.OrderID, "pushed")
					s.log.Infof("Successfully pushed order %s to downstream", dwsOrder.OrderID)
				}
			}
		}

		// 更新游标
		cursor = nextCursor
		task.Cursor = cursor
		_, err = s.etlTaskRepo.UpdateTask(ctx, task)
		if err != nil {
			s.log.Errorf("Failed to update task cursor: %v", err)
		}

		// 如果返回的数据少于批次大小，说明已经处理完所有数据
		if len(dwdOrders) < batchSize {
			break
		}
	}

	// 更新任务状态为完成
	task.Status = ETLTaskStatusCompleted
	task.UpdatedAt = time.Now()
	_, err = s.etlTaskRepo.UpdateTask(ctx, task)
	if err != nil {
		s.log.Errorf("Failed to update task status to completed: %v", err)
	}

	// 更新阶段执行状态
	stageExecution.Status = string(ETLTaskStatusCompleted)
	endedAt := time.Now()
	stageExecution.EndedAt = &endedAt
	_, err = s.etlTaskRepo.UpdateStageExecution(ctx, stageExecution)
	if err != nil {
		s.log.Errorf("Failed to update stage execution: %v", err)
	}

	s.log.Infof("Completed DWD to DWS processing for task %d, processed %d orders", taskID, totalProcessed)
	return nil
}

// updateTaskError 更新任务错误状态
func (s *ETLDWSService) updateTaskError(ctx context.Context, task *ETLTask, stageExecution *ETLStageExecution, err error) {
	s.log.Errorf("DWD to DWS processing error: %v", err)

	// 更新任务状态
	task.Status = ETLTaskStatusFailed
	task.UpdatedAt = time.Now()
	_, updateErr := s.etlTaskRepo.UpdateTask(ctx, task)
	if updateErr != nil {
		s.log.Errorf("Failed to update task status to failed: %v", updateErr)
	}

	// 更新阶段执行状态
	stageExecution.Status = string(ETLTaskStatusFailed)
	endedAt := time.Now()
	stageExecution.EndedAt = &endedAt
	stageExecution.ErrorMsg = err.Error()
	_, updateErr = s.etlTaskRepo.UpdateStageExecution(ctx, stageExecution)
	if updateErr != nil {
		s.log.Errorf("Failed to update stage execution: %v", updateErr)
	}
}

// pushOrderToDownstream 推送订单到下游系统
func (s *ETLDWSService) pushOrderToDownstream(ctx context.Context, dwsOrder *DWSOrder) error {
	// 将DWS订单转换为JSON消息
	messageBody, err := json.Marshal(dwsOrder)
	if err != nil {
		return fmt.Errorf("failed to marshal DWS order: %w", err)
	}

	// 创建RocketMQ消息
	msg := &primitive.Message{
		Topic: "dws_orders_topic", // DWS订单推送主题
		Body:  messageBody,
	}
	// 设置消息标签和键
	msg.WithTag("dws_order")
	msg.WithKeys([]string{dwsOrder.OrderID})

	// 发送消息
	result, err := s.mqProducer.SendSync(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to send message to RocketMQ: %w", err)
	}

	s.log.Infof("Message sent successfully, MessageID: %s, QueueID: %d", result.MsgID, result.MessageQueue.QueueId)
	return nil
}

// UpdateOrderPushStatus 更新订单推送状态
func (s *ETLDWSService) UpdateOrderPushStatus(ctx context.Context, orderID string, status string) error {
	err := s.orderDataRepo.UpdateDWSOrderPushStatus(ctx, orderID, status)
	if err != nil {
		return fmt.Errorf("failed to update order push status: %w", err)
	}
	s.log.Infof("Updated order %s push status to %s", orderID, status)
	return nil
}
