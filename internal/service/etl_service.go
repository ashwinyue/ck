package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	v1 "ck/api/etl/v1"
	"ck/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ETLService ETL任务服务实现
type ETLService struct {
	v1.UnimplementedETLServiceServer

	etlUsecase  *biz.ETLTaskUsecase
	etlExecutor *ETLExecutor
	log         *log.Helper
}

// NewETLService 创建ETL服务
func NewETLService(etlUsecase *biz.ETLTaskUsecase, etlExecutor *ETLExecutor, logger log.Logger) *ETLService {
	return &ETLService{
		etlUsecase:  etlUsecase,
		etlExecutor: etlExecutor,
		log:         log.NewHelper(logger),
	}
}

// CreateTask 创建ETL任务
func (s *ETLService) CreateTask(ctx context.Context, req *v1.CreateTaskRequest) (*v1.CreateTaskReply, error) {
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
		CurrentStage: s.convertProtoStageToEntity(req.StartStage),
		Status:       biz.ETLTaskStatusPending,
		MaxRetries:   int(req.MaxRetries),
		RetryCount:   0,
	}

	// 如果指定了触发时间，设置调度
	if req.ScheduledAt != nil {
		task.Schedule = fmt.Sprintf("scheduled_at_%d", req.ScheduledAt.AsTime().Unix())
	}

	createdTask, err := s.etlUsecase.CreateTask(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	s.log.Infof("Created ETL task: %d, name: %s, start_stage: %s, end_stage: %s",
		createdTask.ID, createdTask.Name, req.StartStage, req.EndStage)

	return &v1.CreateTaskReply{
		TaskId:      createdTask.ID,
		Name:        createdTask.Name,
		Description: createdTask.Description,
		Schedule:    createdTask.Schedule,
		Status:      s.convertEntityStatusToProto(createdTask.Status),
		CreatedAt:   timestamppb.New(createdTask.CreatedAt),
	}, nil
}

// ListTasks 列出ETL任务
func (s *ETLService) ListTasks(ctx context.Context, req *v1.ListTasksRequest) (*v1.ListTasksReply, error) {
	var status biz.ETLTaskStatus
	if req.Status != v1.ETLTaskStatus_ETL_TASK_STATUS_UNSPECIFIED {
		status = s.convertProtoStatusToEntity(req.Status)
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 20
	}

	tasks, err := s.etlUsecase.ListTasks(ctx, status, limit, int(req.Offset))
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}

	taskInfos := make([]*v1.TaskInfo, len(tasks))
	for i, task := range tasks {
		taskInfos[i] = s.convertEntityToTaskInfo(task)
	}

	return &v1.ListTasksReply{
		Tasks: taskInfos,
		Total: int32(len(taskInfos)),
	}, nil
}

// GetTask 获取ETL任务详情
func (s *ETLService) GetTask(ctx context.Context, req *v1.GetTaskRequest) (*v1.GetTaskReply, error) {
	task, err := s.etlUsecase.GetTask(ctx, req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	return &v1.GetTaskReply{
		Task: s.convertEntityToTaskInfo(task),
	}, nil
}

// UpdateTask 更新ETL任务
func (s *ETLService) UpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskReply, error) {
	// 获取现有任务
	existingTask, err := s.etlUsecase.GetTask(ctx, req.TaskId)
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
		existingTask.MaxRetries = int(req.MaxRetries)
	}
	if req.Config != nil {
		configStr, err := s.buildTaskConfig(req.Config, s.convertEntityStageToProto(existingTask.CurrentStage), s.convertEntityStageToProto(existingTask.CurrentStage))
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

	s.log.Infof("Updated ETL task: %d", req.TaskId)

	return &v1.UpdateTaskReply{
		Task: s.convertEntityToTaskInfo(updatedTask),
	}, nil
}

// ExecuteTask 执行ETL任务
func (s *ETLService) ExecuteTask(ctx context.Context, req *v1.ExecuteTaskRequest) (*v1.ExecuteTaskReply, error) {
	// 如果指定了执行时间，检查是否到时间
	if req.ScheduledAt != nil {
		if time.Now().Before(req.ScheduledAt.AsTime()) {
			return &v1.ExecuteTaskReply{
				TaskId:  req.TaskId,
				Status:  v1.ETLTaskStatus_ETL_TASK_STATUS_PENDING,
				Message: fmt.Sprintf("Task scheduled to execute at %s", req.ScheduledAt.AsTime().Format(time.RFC3339)),
			}, nil
		}
	}

	// 执行任务
	if err := s.etlExecutor.ExecuteTask(ctx, req.TaskId); err != nil {
		return nil, fmt.Errorf("failed to execute task: %w", err)
	}

	// 获取更新后的任务状态
	updatedTask, err := s.etlUsecase.GetTask(ctx, req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to get updated task: %w", err)
	}

	s.log.Infof("Executed ETL task: %d, status: %s", req.TaskId, updatedTask.Status)

	reply := &v1.ExecuteTaskReply{
		TaskId:  req.TaskId,
		Status:  s.convertEntityStatusToProto(updatedTask.Status),
		Message: "Task execution started successfully",
	}

	if updatedTask.StartedAt != nil {
		reply.StartedAt = timestamppb.New(*updatedTask.StartedAt)
	}

	return reply, nil
}

// StopTask 停止ETL任务
func (s *ETLService) StopTask(ctx context.Context, req *v1.StopTaskRequest) (*v1.StopTaskReply, error) {
	err := s.etlExecutor.StopTask(ctx, req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to stop task: %w", err)
	}

	s.log.Infof("Stopped ETL task: %d", req.TaskId)

	return &v1.StopTaskReply{
		TaskId:  req.TaskId,
		Status:  v1.ETLTaskStatus_ETL_TASK_STATUS_STOPPED,
		Message: "Task stopped successfully",
	}, nil
}

// GetTaskStatus 获取任务状态
func (s *ETLService) GetTaskStatus(ctx context.Context, req *v1.GetTaskStatusRequest) (*v1.GetTaskStatusReply, error) {
	task, err := s.etlUsecase.GetTask(ctx, req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	reply := &v1.GetTaskStatusReply{
		TaskId:       task.ID,
		Status:       s.convertEntityStatusToProto(task.Status),
		CurrentStage: s.convertEntityStageToProto(task.CurrentStage),
		ErrorMessage: task.ErrorMessage,
	}

	if task.StartedAt != nil {
		reply.StartedAt = timestamppb.New(*task.StartedAt)
	}
	if task.CompletedAt != nil {
		reply.CompletedAt = timestamppb.New(*task.CompletedAt)
	}

	return reply, nil
}

// 辅助方法：转换实体到TaskInfo
func (s *ETLService) convertEntityToTaskInfo(task *biz.ETLTask) *v1.TaskInfo {
	taskInfo := &v1.TaskInfo{
		Id:           task.ID,
		Name:         task.Name,
		Description:  task.Description,
		Schedule:     task.Schedule,
		Status:       s.convertEntityStatusToProto(task.Status),
		CurrentStage: s.convertEntityStageToProto(task.CurrentStage),
		CreatedAt:    timestamppb.New(task.CreatedAt),
		UpdatedAt:    timestamppb.New(task.UpdatedAt),
		ErrorMessage: task.ErrorMessage,
	}

	if task.StartedAt != nil {
		taskInfo.StartedAt = timestamppb.New(*task.StartedAt)
	}
	if task.CompletedAt != nil {
		taskInfo.CompletedAt = timestamppb.New(*task.CompletedAt)
	}

	return taskInfo
}

// 辅助方法：转换proto状态到实体状态
func (s *ETLService) convertProtoStatusToEntity(status v1.ETLTaskStatus) biz.ETLTaskStatus {
	switch status {
	case v1.ETLTaskStatus_ETL_TASK_STATUS_PENDING:
		return biz.ETLTaskStatusPending
	case v1.ETLTaskStatus_ETL_TASK_STATUS_RUNNING:
		return biz.ETLTaskStatusRunning
	case v1.ETLTaskStatus_ETL_TASK_STATUS_COMPLETED:
		return biz.ETLTaskStatusCompleted
	case v1.ETLTaskStatus_ETL_TASK_STATUS_FAILED:
		return biz.ETLTaskStatusFailed
	case v1.ETLTaskStatus_ETL_TASK_STATUS_STOPPED:
		return biz.ETLTaskStatusStopped
	default:
		return biz.ETLTaskStatusPending
	}
}

// 辅助方法：转换实体状态到proto状态
func (s *ETLService) convertEntityStatusToProto(status biz.ETLTaskStatus) v1.ETLTaskStatus {
	switch status {
	case biz.ETLTaskStatusPending:
		return v1.ETLTaskStatus_ETL_TASK_STATUS_PENDING
	case biz.ETLTaskStatusRunning:
		return v1.ETLTaskStatus_ETL_TASK_STATUS_RUNNING
	case biz.ETLTaskStatusCompleted:
		return v1.ETLTaskStatus_ETL_TASK_STATUS_COMPLETED
	case biz.ETLTaskStatusFailed:
		return v1.ETLTaskStatus_ETL_TASK_STATUS_FAILED
	case biz.ETLTaskStatusStopped:
		return v1.ETLTaskStatus_ETL_TASK_STATUS_STOPPED
	default:
		return v1.ETLTaskStatus_ETL_TASK_STATUS_UNSPECIFIED
	}
}

// 辅助方法：转换proto阶段到实体阶段
func (s *ETLService) convertProtoStageToEntity(stage v1.ETLStage) biz.ETLStage {
	switch stage {
	case v1.ETLStage_ETL_STAGE_LANDING_TO_ODS:
		return biz.ETLStageLandingToODS
	case v1.ETLStage_ETL_STAGE_ODS:
		return biz.ETLStageODS
	case v1.ETLStage_ETL_STAGE_DWD:
		return biz.ETLStageDWD
	case v1.ETLStage_ETL_STAGE_DWS:
		return biz.ETLStageDWS
	case v1.ETLStage_ETL_STAGE_DS:
		return biz.ETLStageDS
	default:
		return biz.ETLStageLandingToODS
	}
}

// 辅助方法：转换实体阶段到proto阶段
func (s *ETLService) convertEntityStageToProto(stage biz.ETLStage) v1.ETLStage {
	switch stage {
	case biz.ETLStageLandingToODS:
		return v1.ETLStage_ETL_STAGE_LANDING_TO_ODS
	case biz.ETLStageODS:
		return v1.ETLStage_ETL_STAGE_ODS
	case biz.ETLStageDWD:
		return v1.ETLStage_ETL_STAGE_DWD
	case biz.ETLStageDWS:
		return v1.ETLStage_ETL_STAGE_DWS
	case biz.ETLStageDS:
		return v1.ETLStage_ETL_STAGE_DS
	default:
		return v1.ETLStage_ETL_STAGE_UNSPECIFIED
	}
}

// 辅助方法：验证阶段范围
func (s *ETLService) validateStageRange(startStage, endStage v1.ETLStage) error {
	validStages := []v1.ETLStage{
		v1.ETLStage_ETL_STAGE_LANDING_TO_ODS,
		v1.ETLStage_ETL_STAGE_ODS,
		v1.ETLStage_ETL_STAGE_DWD,
		v1.ETLStage_ETL_STAGE_DWS,
		v1.ETLStage_ETL_STAGE_DS,
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

// 辅助方法：构建任务配置
func (s *ETLService) buildTaskConfig(config map[string]string, startStage, endStage v1.ETLStage) (string, error) {
	// 默认配置
	defaultConfig := map[string]interface{}{
		"batch_size":  1000,
		"start_stage": startStage.String(),
		"end_stage":   endStage.String(),
		"timeout":     "30m",
		"retry_delay": "5m",
	}

	// 合并用户配置，需要特殊处理数值类型
	for k, v := range config {
		switch k {
		case "batch_size":
			// 将字符串转换为整数
			if batchSize, err := strconv.Atoi(v); err == nil {
				defaultConfig[k] = batchSize
			} else {
				return "", fmt.Errorf("invalid batch_size value: %s", v)
			}
		default:
			defaultConfig[k] = v
		}
	}

	// 转换为JSON字符串
	configBytes, err := json.Marshal(defaultConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal config: %w", err)
	}

	return string(configBytes), nil
}
