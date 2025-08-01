package service

import (
	"context"
	"time"

	"ck/internal/biz"
	"ck/internal/pkg/httpclient"

	"github.com/go-kratos/kratos/v2/log"
)

// TaskRunner 任务运行器
type TaskRunner struct {
	task       *biz.ETLTask
	etlUsecase *biz.ETLTaskUsecase
	httpClient *httpclient.Client
	log        *log.Helper
	logger     log.Logger
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
}

// NewTaskRunner 创建任务运行器
func NewTaskRunner(task *biz.ETLTask, etlUsecase *biz.ETLTaskUsecase, httpClient *httpclient.Client, logger log.Logger) *TaskRunner {
	ctx, cancel := context.WithCancel(context.Background())
	return &TaskRunner{
		task:       task,
		etlUsecase: etlUsecase,
		httpClient: httpClient,
		log:        log.NewHelper(logger),
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
}

// Run 运行任务
func (r *TaskRunner) Run() {
	defer close(r.done)
	defer r.log.Infof("Task %d runner stopped", r.task.ID)

	r.log.Infof("Starting task %d runner", r.task.ID)

	// 启动任务
	if err := r.etlUsecase.StartTask(r.ctx, r.task.ID); err != nil {
		r.log.Errorf("Failed to start task %d: %v", r.task.ID, err)
		return
	}

	// 执行任务循环
	for {
		select {
		case <-r.ctx.Done():
			r.log.Infof("Task %d runner context cancelled", r.task.ID)
			return
		default:
			// 获取最新任务状态
			task, err := r.etlUsecase.GetTask(r.ctx, r.task.ID)
			if err != nil {
				r.log.Errorf("Failed to get task %d: %v", r.task.ID, err)
				return
			}

			r.task = task

			// 检查任务状态
			if task.Status == biz.ETLTaskStatusCompleted || task.Status == biz.ETLTaskStatusFailed {
				r.log.Infof("Task %d finished with status: %s", r.task.ID, task.Status)
				return
			}

			if task.Status == biz.ETLTaskStatusPaused {
				r.log.Infof("Task %d is paused, waiting...", r.task.ID)
				time.Sleep(5 * time.Second)
				continue
			}

			// 执行当前阶段
			if err := r.executeCurrentStage(); err != nil {
				r.log.Errorf("Failed to execute stage %s for task %d: %v", task.CurrentStage, r.task.ID, err)
				if err := r.etlUsecase.FailTask(r.ctx, r.task.ID, err.Error()); err != nil {
					r.log.Errorf("Failed to mark task %d as failed: %v", r.task.ID, err)
				}
				return
			}

			// 完成当前阶段
			if err := r.etlUsecase.CompleteStage(r.ctx, r.task.ID); err != nil {
				r.log.Errorf("Failed to complete stage for task %d: %v", r.task.ID, err)
				return
			}

			// 短暂休息
			time.Sleep(1 * time.Second)
		}
	}
}

// Stop 停止任务运行器
func (r *TaskRunner) Stop() {
	r.cancel()
	<-r.done
}

// executeCurrentStage 执行当前阶段
func (r *TaskRunner) executeCurrentStage() error {
	// 解析配置
	config, err := r.etlUsecase.ParseConfig(r.task.Config)
	if err != nil {
		return err
	}

	// 创建阶段执行器
	stageExecutor := NewETLStageExecutor(r.etlUsecase, r.httpClient, r.logger)

	// 根据当前阶段执行相应逻辑
	switch r.task.CurrentStage {
	case biz.ETLStageLandingToODS:
		return stageExecutor.ExecuteLandingToODS(r.ctx, r.task, config)
	case biz.ETLStageODS:
		return stageExecutor.ExecuteODS(r.ctx, r.task, config)
	case biz.ETLStageDWD:
		return stageExecutor.ExecuteDWD(r.ctx, r.task, config)
	case biz.ETLStageDWS:
		return stageExecutor.ExecuteDWS(r.ctx, r.task, config)
	case biz.ETLStageDS:
		return stageExecutor.ExecuteDS(r.ctx, r.task, config)
	default:
		return nil
	}
}
