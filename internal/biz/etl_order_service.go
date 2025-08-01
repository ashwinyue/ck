package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

// ETLOrderService ETL订单处理服务
type ETLOrderService struct {
	etlTaskRepo   ETLTaskRepo
	orderDataRepo OrderDataRepo
	transformer   *OrderTransformer
	log           *log.Helper
}

// NewETLOrderService 创建ETL订单处理服务
func NewETLOrderService(
	etlTaskRepo ETLTaskRepo,
	orderDataRepo OrderDataRepo,
	logger log.Logger,
) *ETLOrderService {
	return &ETLOrderService{
		etlTaskRepo:   etlTaskRepo,
		orderDataRepo: orderDataRepo,
		transformer:   &OrderTransformer{},
		log:           log.NewHelper(logger),
	}
}

// ProcessODSToDWD 处理ODS到DWD的数据转换
func (s *ETLOrderService) ProcessODSToDWD(ctx context.Context, taskID int64, batchSize int) error {
	// 获取ETL任务
	task, err := s.etlTaskRepo.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get ETL task: %w", err)
	}

	if task == nil {
		return fmt.Errorf("ETL task not found: %d", taskID)
	}

	// 更新任务状态为运行中
	task.Status = ETLTaskStatusRunning
	task.CurrentStage = ETLStageDWD
	task.UpdatedAt = time.Now()

	_, err = s.etlTaskRepo.UpdateTask(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	// 创建阶段执行记录
	stageExecution := &ETLStageExecution{
		TaskID:    taskID,
		Stage:     ETLStageDWD,
		Status:    "running",
		StartedAt: time.Now(),
	}

	_, err = s.etlTaskRepo.CreateStageExecution(ctx, stageExecution)
	if err != nil {
		return fmt.Errorf("failed to create stage execution: %w", err)
	}

	// 开始处理数据
	totalProcessed := 0
	cursor := task.Cursor // 从任务游标开始

	for {
		// 获取ODS数据
		odsOrders, nextCursor, err := s.orderDataRepo.GetODSOrdersByCursor(ctx, cursor, batchSize)
		if err != nil {
			s.updateTaskError(ctx, task, stageExecution, fmt.Errorf("failed to get ODS orders: %w", err))
			return err
		}

		if len(odsOrders) == 0 {
			// 没有更多数据，处理完成
			break
		}

		// 转换数据
		var dwdOrders []*DWDOrder
		for _, odsOrder := range odsOrders {
			dwdOrder, err := s.transformer.TransformODSToDWD(odsOrder)
			if err != nil {
				s.log.Warnf("Failed to transform order %s: %v", odsOrder.OrderID, err)
				continue
			}

			// 只处理已完成的订单
			if dwdOrder != nil {
				dwdOrders = append(dwdOrders, dwdOrder)
			}
		}

		// 批量插入DWD数据
		if len(dwdOrders) > 0 {
			err = s.orderDataRepo.BatchInsertDWDOrders(ctx, dwdOrders)
			if err != nil {
				s.updateTaskError(ctx, task, stageExecution, fmt.Errorf("failed to insert DWD orders: %w", err))
				return err
			}
		}

		// 更新游标和进度
		task.Cursor = nextCursor
		totalProcessed += len(odsOrders)
		cursor = nextCursor

		// 保存进度
		_, err = s.etlTaskRepo.UpdateTask(ctx, task)
		if err != nil {
			s.log.Warnf("Failed to update task cursor: %v", err)
		}

		s.log.Infof("Processed batch: %d orders, %d completed orders inserted, total processed: %d",
			len(odsOrders), len(dwdOrders), totalProcessed)
	}

	// 更新任务和阶段状态为完成
	task.Status = ETLTaskStatusCompleted
	task.UpdatedAt = time.Now()

	stageExecution.Status = "completed"
	endTime := time.Now()
	stageExecution.EndedAt = &endTime

	_, err = s.etlTaskRepo.UpdateTask(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to update task completion status: %w", err)
	}

	_, err = s.etlTaskRepo.UpdateStageExecution(ctx, stageExecution)
	if err != nil {
		return fmt.Errorf("failed to update stage execution: %w", err)
	}

	s.log.Infof("ETL task %s completed successfully, total processed: %d orders", taskID, totalProcessed)
	return nil
}

// updateTaskError 更新任务和阶段的错误状态
func (s *ETLOrderService) updateTaskError(ctx context.Context, task *ETLTask, stageExecution *ETLStageExecution, err error) {
	task.Status = ETLTaskStatusFailed
	task.ErrorMessage = err.Error()
	task.UpdatedAt = time.Now()

	stageExecution.Status = "failed"
	stageExecution.ErrorMsg = err.Error()
	endTime := time.Now()
	stageExecution.EndedAt = &endTime

	if _, updateErr := s.etlTaskRepo.UpdateTask(ctx, task); updateErr != nil {
		s.log.Errorf("Failed to update task error status: %v", updateErr)
	}

	if _, updateErr := s.etlTaskRepo.UpdateStageExecution(ctx, stageExecution); updateErr != nil {
		s.log.Errorf("Failed to update stage execution error status: %v", updateErr)
	}
}

// CreateODSToDWDTask 创建ODS到DWD的ETL任务
func (s *ETLOrderService) CreateODSToDWDTask(ctx context.Context, name, description string) (*ETLTask, error) {
	task := &ETLTask{
		Name:         name,
		Description:  description,
		Schedule:     "manual", // 手动触发
		Config:       `{"batch_size": 1000, "source_table": "ods_orders", "target_table": "dwd_orders"}`,
		Status:       ETLTaskStatusPending,
		CurrentStage: ETLStageODS,
		Cursor:       "", // 从头开始
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		RetryCount:   0,
		MaxRetries:   3,
	}

	createdTask, err := s.etlTaskRepo.CreateTask(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to create ETL task: %w", err)
	}

	return createdTask, nil
}
