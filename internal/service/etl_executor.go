package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ck/internal/biz"
	"ck/internal/pkg/distlock"
	"ck/internal/pkg/httpclient"

	"github.com/go-kratos/kratos/v2/log"
)

// ETLExecutor ETL执行器
type ETLExecutor struct {
	etlUsecase  *biz.ETLTaskUsecase
	log         *log.Helper
	logger      log.Logger
	mu          sync.RWMutex
	running     map[int64]*TaskRunner // 正在运行的任务
	ctx         context.Context
	cancel      context.CancelFunc
	lockFactory *distlock.Factory
	httpClient  *httpclient.Client
}

// NewETLExecutor 创建ETL执行器
func NewETLExecutor(etlUsecase *biz.ETLTaskUsecase, lockFactory *distlock.Factory, httpClient *httpclient.Client, logger log.Logger) *ETLExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	return &ETLExecutor{
		etlUsecase:  etlUsecase,
		log:         log.NewHelper(logger),
		logger:      logger,
		running:     make(map[int64]*TaskRunner),
		ctx:         ctx,
		cancel:      cancel,
		lockFactory: lockFactory,
		httpClient:  httpClient,
	}
}

// Start 启动执行器
func (e *ETLExecutor) Start() error {
	e.log.Info("Starting ETL executor")
	return nil
}

// Stop 停止执行器
func (e *ETLExecutor) Stop() error {
	e.log.Info("Stopping ETL executor")
	e.cancel()

	// 停止所有正在运行的任务
	e.mu.Lock()
	for _, runner := range e.running {
		runner.cancel()
		<-runner.done
	}
	e.running = make(map[int64]*TaskRunner)
	e.mu.Unlock()

	return nil
}

// ExecuteTask 执行任务
func (e *ETLExecutor) ExecuteTask(ctx context.Context, taskID int64) error {
	e.log.Infof("Starting execution of task %d", taskID)

	// 使用分布式锁确保任务不会重复执行
	lockName := fmt.Sprintf("etl_task_%d", taskID)
	locker := e.lockFactory.CreateRedsyncLocker(
		distlock.WithLockName(lockName),
		distlock.WithLockTimeout(30*time.Minute), // 任务最长执行时间
		distlock.WithLogger(e.log),
	)

	// 尝试获取锁
	if err := locker.Lock(ctx); err != nil {
		return fmt.Errorf("failed to acquire lock for task %d: %w", taskID, err)
	}

	// 检查任务是否已在运行
	e.mu.RLock()
	if _, exists := e.running[taskID]; exists {
		e.mu.RUnlock()
		locker.Unlock(ctx)
		return fmt.Errorf("task %d is already running", taskID)
	}
	e.mu.RUnlock()

	// 获取任务
	task, err := e.etlUsecase.GetTask(ctx, taskID)
	if err != nil {
		locker.Unlock(ctx)
		return fmt.Errorf("failed to get task %d: %w", taskID, err)
	}

	// 检查任务状态
	if task.Status != biz.ETLTaskStatusPending && task.Status != biz.ETLTaskStatusPaused {
		locker.Unlock(ctx)
		return fmt.Errorf("task %d is not in pending or paused state, current status: %s", taskID, task.Status)
	}

	// 启动任务
	err = e.etlUsecase.StartTask(ctx, taskID)
	if err != nil {
		locker.Unlock(ctx)
		return fmt.Errorf("failed to start task %d: %w", taskID, err)
	}

	// 创建任务运行器
	runner := NewTaskRunner(task, e.etlUsecase, e.httpClient, e.logger)

	// 注册运行器
	e.mu.Lock()
	e.running[taskID] = runner
	e.mu.Unlock()

	// 异步执行任务
	go func() {
		defer func() {
			// 释放分布式锁
			if err := locker.Unlock(ctx); err != nil {
				e.log.Errorf("failed to release lock for task %d: %v", taskID, err)
			}
		}()

		// 定期续期锁
		renewCtx, renewCancel := context.WithCancel(ctx)
		defer renewCancel()
		go func() {
			ticker := time.NewTicker(10 * time.Minute) // 每10分钟续期一次
			defer ticker.Stop()
			for {
				select {
				case <-renewCtx.Done():
					return
				case <-ticker.C:
					if err := locker.Renew(ctx); err != nil {
						e.log.Errorf("failed to renew lock for task %d: %v", taskID, err)
					}
				}
			}
		}()

		runner.Run()
	}()

	return nil
}

// PauseTask 暂停任务
func (e *ETLExecutor) PauseTask(ctx context.Context, taskID int64) error {
	e.log.Infof("Pausing task %d", taskID)

	// 暂停任务状态
	err := e.etlUsecase.PauseTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to pause task %d: %w", taskID, err)
	}

	// 停止运行器
	e.mu.Lock()
	if runner, exists := e.running[taskID]; exists {
		runner.cancel()
		delete(e.running, taskID)
	}
	e.mu.Unlock()

	return nil
}

// ResumeTask 恢复任务
func (e *ETLExecutor) ResumeTask(ctx context.Context, taskID int64) error {
	e.log.Infof("Resuming task %d", taskID)
	return e.ExecuteTask(ctx, taskID)
}

// StopTask 停止任务
func (e *ETLExecutor) StopTask(ctx context.Context, taskID int64) error {
	e.log.Infof("Stopping task %d", taskID)

	// 停止运行器
	e.mu.Lock()
	if runner, exists := e.running[taskID]; exists {
		runner.cancel()
		<-runner.done
		delete(e.running, taskID)
	}
	e.mu.Unlock()

	return nil
}

// GetRunningTasks 获取正在运行的任务
func (e *ETLExecutor) GetRunningTasks() []int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	taskIDs := make([]int64, 0, len(e.running))
	for taskID := range e.running {
		taskIDs = append(taskIDs, taskID)
	}
	return taskIDs
}

// GetTaskStatus 获取任务状态
func (e *ETLExecutor) GetTaskStatus(ctx context.Context, taskID int64) (*biz.ETLTask, error) {
	return e.etlUsecase.GetTask(ctx, taskID)
}

// ListTasks 列出任务
func (e *ETLExecutor) ListTasks(ctx context.Context, status biz.ETLTaskStatus, limit, offset int) ([]*biz.ETLTask, error) {
	return e.etlUsecase.ListTasks(ctx, status, limit, offset)
}

// CreateTask 创建任务
func (e *ETLExecutor) CreateTask(ctx context.Context, task *biz.ETLTask) (*biz.ETLTask, error) {
	return e.etlUsecase.CreateTask(ctx, task)
}

// GetStageExecutions 获取阶段执行记录
func (e *ETLExecutor) GetStageExecutions(ctx context.Context, taskID int64, limit int) ([]*biz.ETLStageExecution, error) {
	return e.etlUsecase.GetStageExecutions(ctx, taskID, limit)
}
