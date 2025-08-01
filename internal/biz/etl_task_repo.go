package biz

import "context"

// ETLTaskRepo ETL任务仓储接口
type ETLTaskRepo interface {
	// 任务管理
	CreateTask(ctx context.Context, task *ETLTask) (*ETLTask, error)
	UpdateTask(ctx context.Context, task *ETLTask) (*ETLTask, error)
	DeleteTask(ctx context.Context, id int64) error
	GetTask(ctx context.Context, id int64) (*ETLTask, error)
	ListTasks(ctx context.Context, status ETLTaskStatus, limit, offset int) ([]*ETLTask, error)
	GetTasksBySchedule(ctx context.Context, schedule string) ([]*ETLTask, error)

	// 阶段执行管理
	CreateStageExecution(ctx context.Context, execution *ETLStageExecution) (*ETLStageExecution, error)
	UpdateStageExecution(ctx context.Context, execution *ETLStageExecution) (*ETLStageExecution, error)
	GetStageExecutions(ctx context.Context, taskID int64, limit int) ([]*ETLStageExecution, error)
	GetLatestStageExecution(ctx context.Context, taskID int64, stage ETLStage) (*ETLStageExecution, error)
}
