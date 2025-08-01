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

// OrderDataRepo 订单数据仓储接口
type OrderDataRepo interface {
	// ODS层订单数据操作
	GetODSOrdersByCursor(ctx context.Context, cursor string, limit int) ([]*ODSOrder, string, error)

	// DWD层订单数据操作
	BatchInsertDWDOrders(ctx context.Context, orders []*DWDOrder) error
	GetDWDOrderByOrderID(ctx context.Context, orderID string) (*DWDOrder, error)
	GetDWDOrdersByCursor(ctx context.Context, cursor string, limit int) ([]*DWDOrder, string, error)

	// DWS层订单数据操作
	BatchInsertDWSOrders(ctx context.Context, orders []*DWSOrder) error
	GetDWSOrderByOrderID(ctx context.Context, orderID string) (*DWSOrder, error)
	UpdateDWSOrderPushStatus(ctx context.Context, orderID string, status string) error
}
