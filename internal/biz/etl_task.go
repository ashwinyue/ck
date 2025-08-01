package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/looplab/fsm"
)

// ETLTaskUsecase ETL任务用例
type ETLTaskUsecase struct {
	repo   ETLTaskRepo
	log    *log.Helper
	logger log.Logger
}

// NewETLTaskUsecase 创建ETL任务用例
func NewETLTaskUsecase(repo ETLTaskRepo, logger log.Logger) *ETLTaskUsecase {
	return &ETLTaskUsecase{
		repo:   repo,
		log:    log.NewHelper(logger),
		logger: logger,
	}
}

// CreateTask 创建任务
func (uc *ETLTaskUsecase) CreateTask(ctx context.Context, task *ETLTask) (*ETLTask, error) {
	// 初始化任务状态
	task.CurrentStage = ETLStageLandingToODS
	task.Status = ETLTaskStatusPending
	task.RetryCount = 0

	// 创建状态机
	task.FSM = uc.createFSM(task)

	return uc.repo.CreateTask(ctx, task)
}

// UpdateTask 更新任务
func (uc *ETLTaskUsecase) UpdateTask(ctx context.Context, task *ETLTask) (*ETLTask, error) {
	// 重新创建状态机
	task.FSM = uc.createFSM(task)
	return uc.repo.UpdateTask(ctx, task)
}

// GetTask 获取任务
func (uc *ETLTaskUsecase) GetTask(ctx context.Context, id int64) (*ETLTask, error) {
	task, err := uc.repo.GetTask(ctx, id)
	if err != nil {
		return nil, err
	}

	// 重新创建状态机
	task.FSM = uc.createFSM(task)
	return task, nil
}

// ListTasks 列出任务
func (uc *ETLTaskUsecase) ListTasks(ctx context.Context, status ETLTaskStatus, limit, offset int) ([]*ETLTask, error) {
	tasks, err := uc.repo.ListTasks(ctx, status, limit, offset)
	if err != nil {
		return nil, err
	}

	// 为每个任务重新创建状态机
	for _, task := range tasks {
		task.FSM = uc.createFSM(task)
	}

	return tasks, nil
}

// StartTask 启动任务
func (uc *ETLTaskUsecase) StartTask(ctx context.Context, taskID int64) error {
	task, err := uc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.FSM.Event(ctx, string(ETLEventStart))
	if err != nil {
		return fmt.Errorf("failed to start task: %w", err)
	}

	// 更新任务状态
	task.Status = ETLTaskStatus(task.FSM.Current())
	now := time.Now()
	task.StartedAt = &now
	task.UpdatedAt = now

	_, err = uc.repo.UpdateTask(ctx, task)
	return err
}

// PauseTask 暂停任务
func (uc *ETLTaskUsecase) PauseTask(ctx context.Context, taskID int64) error {
	task, err := uc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.FSM.Event(ctx, string(ETLEventPause))
	if err != nil {
		return fmt.Errorf("failed to pause task: %w", err)
	}

	// 更新任务状态
	task.Status = ETLTaskStatus(task.FSM.Current())
	task.UpdatedAt = time.Now()

	_, err = uc.repo.UpdateTask(ctx, task)
	return err
}

// ResumeTask 恢复任务
func (uc *ETLTaskUsecase) ResumeTask(ctx context.Context, taskID int64) error {
	task, err := uc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.FSM.Event(ctx, string(ETLEventResume))
	if err != nil {
		return fmt.Errorf("failed to resume task: %w", err)
	}

	// 更新任务状态
	task.Status = ETLTaskStatus(task.FSM.Current())
	task.UpdatedAt = time.Now()

	_, err = uc.repo.UpdateTask(ctx, task)
	return err
}

// CompleteStage 完成当前阶段
func (uc *ETLTaskUsecase) CompleteStage(ctx context.Context, taskID int64) error {
	task, err := uc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	// 记录当前阶段完成
	currentStage := task.CurrentStage

	err = task.FSM.Event(ctx, string(ETLEventStageComplete))
	if err != nil {
		return fmt.Errorf("failed to complete stage: %w", err)
	}

	// 更新任务状态和阶段
	task.CurrentStage = ETLStage(task.FSM.Current())
	if task.CurrentStage == ETLStageCompleted {
		task.Status = ETLTaskStatusCompleted
		now := time.Now()
		task.CompletedAt = &now
	}
	task.UpdatedAt = time.Now()

	// 创建阶段执行记录
	stageExecution := &ETLStageExecution{
		TaskID:    taskID,
		Stage:     currentStage,
		Status:    "completed",
		StartedAt: time.Now(), // 这里应该从实际执行开始时间获取
		EndedAt:   &task.UpdatedAt,
	}

	_, err = uc.repo.CreateStageExecution(ctx, stageExecution)
	if err != nil {
		uc.log.Errorf("Failed to create stage execution record: %v", err)
	}

	_, err = uc.repo.UpdateTask(ctx, task)
	return err
}

// FailTask 任务失败
func (uc *ETLTaskUsecase) FailTask(ctx context.Context, taskID int64, errorMsg string) error {
	task, err := uc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.FSM.Event(ctx, string(ETLEventFail))
	if err != nil {
		return fmt.Errorf("failed to fail task: %w", err)
	}

	// 更新任务状态
	task.Status = ETLTaskStatus(task.FSM.Current())
	task.ErrorMessage = errorMsg
	task.RetryCount++
	task.UpdatedAt = time.Now()

	_, err = uc.repo.UpdateTask(ctx, task)
	return err
}

// ResetTask 重置任务
func (uc *ETLTaskUsecase) ResetTask(ctx context.Context, taskID int64) error {
	task, err := uc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	err = task.FSM.Event(ctx, string(ETLEventReset))
	if err != nil {
		return fmt.Errorf("failed to reset task: %w", err)
	}

	// 重置任务状态
	task.CurrentStage = ETLStageLandingToODS
	task.Status = ETLTaskStatusPending
	task.RetryCount = 0
	task.ErrorMessage = ""
	task.StartedAt = nil
	task.CompletedAt = nil
	task.UpdatedAt = time.Now()

	_, err = uc.repo.UpdateTask(ctx, task)
	return err
}

// GetTasksBySchedule 根据调度获取任务
func (uc *ETLTaskUsecase) GetTasksBySchedule(ctx context.Context, schedule string) ([]*ETLTask, error) {
	tasks, err := uc.repo.GetTasksBySchedule(ctx, schedule)
	if err != nil {
		return nil, err
	}

	// 为每个任务重新创建状态机
	for _, task := range tasks {
		task.FSM = uc.createFSM(task)
	}

	return tasks, nil
}

// GetStageExecutions 获取阶段执行记录
func (uc *ETLTaskUsecase) GetStageExecutions(ctx context.Context, taskID int64, limit int) ([]*ETLStageExecution, error) {
	return uc.repo.GetStageExecutions(ctx, taskID, limit)
}

// ParseConfig 解析任务配置
func (uc *ETLTaskUsecase) ParseConfig(configStr string) (*ETLTaskConfig, error) {
	configManager := NewETLTaskConfigManager(uc.logger)
	return configManager.ParseConfig(configStr)
}

// createFSM 创建状态机
func (uc *ETLTaskUsecase) createFSM(task *ETLTask) *fsm.FSM {
	fsmManager := NewETLTaskFSMManager(uc.logger)
	return fsmManager.CreateFSM(task)
}
