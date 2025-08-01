package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ck/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
)

// ETLAPIService ETL任务API服务
type ETLAPIService struct {
	etlUsecase  *biz.ETLTaskUsecase
	etlExecutor *ETLExecutor
	log         *log.Helper
}

// NewETLAPIService 创建ETL API服务
func NewETLAPIService(etlUsecase *biz.ETLTaskUsecase, etlExecutor *ETLExecutor, logger log.Logger) *ETLAPIService {
	return &ETLAPIService{
		etlUsecase:  etlUsecase,
		etlExecutor: etlExecutor,
		log:         log.NewHelper(logger),
	}
}

// CreateTaskRequest 创建任务请求
type CreateTaskRequest struct {
	Name        string            `json:"name" binding:"required"`
	Description string            `json:"description"`
	Schedule    string            `json:"schedule"`     // cron表达式或"manual"表示手动触发
	StartStage  biz.ETLStage      `json:"start_stage"`  // 开始阶段
	EndStage    biz.ETLStage      `json:"end_stage"`    // 结束阶段
	Config      map[string]string `json:"config"`       // 任务配置
	MaxRetries  int               `json:"max_retries"`  // 最大重试次数
	ScheduledAt *time.Time        `json:"scheduled_at"` // 指定触发时间（可选）
}

// CreateTaskResponse 创建任务响应
type CreateTaskResponse struct {
	TaskID      int64     `json:"task_id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Schedule    string    `json:"schedule"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

// ExecuteTaskRequest 执行任务请求
type ExecuteTaskRequest struct {
	TaskID      int64      `json:"task_id" binding:"required"`
	ScheduledAt *time.Time `json:"scheduled_at"` // 指定执行时间（可选）
}

// ExecuteTaskResponse 执行任务响应
type ExecuteTaskResponse struct {
	TaskID    int64      `json:"task_id"`
	Status    string     `json:"status"`
	Message   string     `json:"message"`
	StartedAt *time.Time `json:"started_at,omitempty"`
}

// ListTasksRequest 列出任务请求
type ListTasksRequest struct {
	Status string `form:"status"`
	Limit  int    `form:"limit"`
	Offset int    `form:"offset"`
}

// ListTasksResponse 列出任务响应
type ListTasksResponse struct {
	Tasks []TaskInfo `json:"tasks"`
	Total int        `json:"total"`
}

// TaskInfo 任务信息
type TaskInfo struct {
	ID           int64      `json:"id"`
	Name         string     `json:"name"`
	Description  string     `json:"description"`
	Schedule     string     `json:"schedule"`
	Status       string     `json:"status"`
	CurrentStage string     `json:"current_stage"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

// GetTaskRequest 获取任务请求
type GetTaskRequest struct {
	TaskID int64 `uri:"task_id" binding:"required"`
}

// GetTaskResponse 获取任务响应
type GetTaskResponse struct {
	Task TaskInfo `json:"task"`
}

// UpdateTaskRequest 更新任务请求
type UpdateTaskRequest struct {
	TaskID      int64             `uri:"task_id" binding:"required"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Schedule    string            `json:"schedule"`
	Config      map[string]string `json:"config"`
	MaxRetries  int               `json:"max_retries"`
}

// UpdateTaskResponse 更新任务响应
type UpdateTaskResponse struct {
	Task TaskInfo `json:"task"`
}

// StopTaskRequest 停止任务请求
type StopTaskRequest struct {
	TaskID int64 `uri:"task_id" binding:"required"`
}

// StopTaskResponse 停止任务响应
type StopTaskResponse struct {
	TaskID  int64  `json:"task_id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// CreateTask 创建ETL任务
func (s *ETLAPIService) CreateTask(ctx context.Context, req *CreateTaskRequest) (*CreateTaskResponse, error) {
	// 验证阶段范围
	if err := s.validateStageRange(req.StartStage, req.EndStage); err != nil {
		return nil, err
	}

	// 构建任务配置
	configStr, err := s.buildTaskConfig(req.Config, req.StartStage, req.EndStage)
	if err != nil {
		return nil, fmt.Errorf("failed to build task config: %w", err)
	}

	// 创建任务实体
	task := &biz.ETLTask{
		Name:         req.Name,
		Description:  req.Description,
		Schedule:     req.Schedule,
		Config:       configStr,
		CurrentStage: req.StartStage,
		Status:       biz.ETLTaskStatusPending,
		MaxRetries:   req.MaxRetries,
		RetryCount:   0,
	}

	// 如果指定了触发时间，设置调度
	if req.ScheduledAt != nil {
		task.Schedule = fmt.Sprintf("scheduled_at_%d", req.ScheduledAt.Unix())
	}

	createdTask, err := s.etlUsecase.CreateTask(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	s.log.Infof("Created ETL task: %d, name: %s, start_stage: %s, end_stage: %s",
		createdTask.ID, createdTask.Name, req.StartStage, req.EndStage)

	return &CreateTaskResponse{
		TaskID:      createdTask.ID,
		Name:        createdTask.Name,
		Description: createdTask.Description,
		Schedule:    createdTask.Schedule,
		Status:      string(createdTask.Status),
		CreatedAt:   createdTask.CreatedAt,
	}, nil
}

// ExecuteTask 执行ETL任务
func (s *ETLAPIService) ExecuteTask(ctx context.Context, req *ExecuteTaskRequest) (*ExecuteTaskResponse, error) {
	// 如果指定了执行时间，检查是否到时间
	if req.ScheduledAt != nil {
		if time.Now().Before(*req.ScheduledAt) {
			return &ExecuteTaskResponse{
				TaskID:  req.TaskID,
				Status:  "scheduled",
				Message: fmt.Sprintf("Task scheduled to execute at %s", req.ScheduledAt.Format(time.RFC3339)),
			}, nil
		}
	}

	// 执行任务
	if err := s.etlExecutor.ExecuteTask(ctx, req.TaskID); err != nil {
		return nil, fmt.Errorf("failed to execute task: %w", err)
	}

	// 获取更新后的任务状态
	updatedTask, err := s.etlUsecase.GetTask(ctx, req.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get updated task: %w", err)
	}

	s.log.Infof("Executed ETL task: %d, status: %s", req.TaskID, updatedTask.Status)

	return &ExecuteTaskResponse{
		TaskID:    req.TaskID,
		Status:    string(updatedTask.Status),
		Message:   "Task execution started successfully",
		StartedAt: updatedTask.StartedAt,
	}, nil
}

// ListTasks 列出ETL任务
func (s *ETLAPIService) ListTasks(ctx context.Context, req *ListTasksRequest) (*ListTasksResponse, error) {
	var status biz.ETLTaskStatus
	if req.Status != "" {
		status = biz.ETLTaskStatus(req.Status)
	}

	if req.Limit <= 0 {
		req.Limit = 20
	}

	tasks, err := s.etlUsecase.ListTasks(ctx, status, req.Limit, req.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}

	taskInfos := make([]TaskInfo, len(tasks))
	for i, task := range tasks {
		taskInfos[i] = TaskInfo{
			ID:           task.ID,
			Name:         task.Name,
			Description:  task.Description,
			Schedule:     task.Schedule,
			Status:       string(task.Status),
			CurrentStage: string(task.CurrentStage),
			CreatedAt:    task.CreatedAt,
			UpdatedAt:    task.UpdatedAt,
			StartedAt:    task.StartedAt,
			CompletedAt:  task.CompletedAt,
			ErrorMessage: task.ErrorMessage,
		}
	}

	return &ListTasksResponse{
		Tasks: taskInfos,
		Total: len(taskInfos),
	}, nil
}

// GetTask 获取ETL任务详情
func (s *ETLAPIService) GetTask(ctx context.Context, req *GetTaskRequest) (*GetTaskResponse, error) {
	task, err := s.etlUsecase.GetTask(ctx, req.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	return &GetTaskResponse{
		Task: TaskInfo{
			ID:           task.ID,
			Name:         task.Name,
			Description:  task.Description,
			Schedule:     task.Schedule,
			Status:       string(task.Status),
			CurrentStage: string(task.CurrentStage),
			CreatedAt:    task.CreatedAt,
			UpdatedAt:    task.UpdatedAt,
			StartedAt:    task.StartedAt,
			CompletedAt:  task.CompletedAt,
			ErrorMessage: task.ErrorMessage,
		},
	}, nil
}

// UpdateTask 更新ETL任务
func (s *ETLAPIService) UpdateTask(ctx context.Context, req *UpdateTaskRequest) (*UpdateTaskResponse, error) {
	// 获取现有任务
	existingTask, err := s.etlUsecase.GetTask(ctx, req.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing task: %w", err)
	}

	// 更新任务字段
	if req.Name != "" {
		existingTask.Name = req.Name
	}
	if req.Description != "" {
		existingTask.Description = req.Description
	}
	if req.Schedule != "" {
		existingTask.Schedule = req.Schedule
	}
	if req.MaxRetries > 0 {
		existingTask.MaxRetries = req.MaxRetries
	}
	if req.Config != nil {
		configStr, err := s.buildTaskConfig(req.Config, existingTask.CurrentStage, existingTask.CurrentStage)
		if err != nil {
			return nil, fmt.Errorf("failed to build task config: %w", err)
		}
		existingTask.Config = configStr
	}

	// 更新任务
	updatedTask, err := s.etlUsecase.UpdateTask(ctx, existingTask)
	if err != nil {
		return nil, fmt.Errorf("failed to update task: %w", err)
	}

	s.log.Infof("Updated ETL task: %d", req.TaskID)

	return &UpdateTaskResponse{
		Task: TaskInfo{
			ID:           updatedTask.ID,
			Name:         updatedTask.Name,
			Description:  updatedTask.Description,
			Schedule:     updatedTask.Schedule,
			Status:       string(updatedTask.Status),
			CurrentStage: string(updatedTask.CurrentStage),
			CreatedAt:    updatedTask.CreatedAt,
			UpdatedAt:    updatedTask.UpdatedAt,
			StartedAt:    updatedTask.StartedAt,
			CompletedAt:  updatedTask.CompletedAt,
			ErrorMessage: updatedTask.ErrorMessage,
		},
	}, nil
}

// StopTask 停止ETL任务
func (s *ETLAPIService) StopTask(ctx context.Context, req *StopTaskRequest) (*StopTaskResponse, error) {
	err := s.etlExecutor.StopTask(ctx, req.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to stop task: %w", err)
	}

	s.log.Infof("Stopped ETL task: %d", req.TaskID)

	return &StopTaskResponse{
		TaskID:  req.TaskID,
		Status:  "stopped",
		Message: "Task stopped successfully",
	}, nil
}

// validateStageRange 验证阶段范围
func (s *ETLAPIService) validateStageRange(startStage, endStage biz.ETLStage) error {
	validStages := []biz.ETLStage{
		biz.ETLStageLandingToODS,
		biz.ETLStageODS,
		biz.ETLStageDWD,
		biz.ETLStageDWS,
		biz.ETLStageDS,
	}

	startIndex := -1
	endIndex := -1

	for i, stage := range validStages {
		if stage == startStage {
			startIndex = i
		}
		if stage == endStage {
			endIndex = i
		}
	}

	if startIndex == -1 {
		return fmt.Errorf("invalid start stage: %s", startStage)
	}
	if endIndex == -1 {
		return fmt.Errorf("invalid end stage: %s", endStage)
	}
	if startIndex > endIndex {
		return fmt.Errorf("start stage (%s) cannot be after end stage (%s)", startStage, endStage)
	}

	return nil
}

// buildTaskConfig 构建任务配置
func (s *ETLAPIService) buildTaskConfig(config map[string]string, startStage, endStage biz.ETLStage) (string, error) {
	// 默认配置
	defaultConfig := map[string]interface{}{
		"batch_size":  1000,
		"start_stage": string(startStage),
		"end_stage":   string(endStage),
		"timeout":     "30m",
		"retry_delay": "5m",
	}

	// 合并用户配置
	for k, v := range config {
		defaultConfig[k] = v
	}

	// 转换为JSON字符串
	configBytes, err := json.Marshal(defaultConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal config: %w", err)
	}

	return string(configBytes), nil
}
