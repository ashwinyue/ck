package data

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ck/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
)

// etlTaskRepo ETL任务仓储实现（内存版本，用于演示）
type etlTaskRepo struct {
	tasks           map[int64]*biz.ETLTask
	stageExecutions map[int64]*biz.ETLStageExecution
	nextTaskID      int64
	nextExecID      int64
	mu              sync.RWMutex
	log             *log.Helper
}

// NewETLTaskRepo 创建ETL任务仓储
func NewETLTaskRepo(logger log.Logger) biz.ETLTaskRepo {
	repo := &etlTaskRepo{
		tasks:           make(map[int64]*biz.ETLTask),
		stageExecutions: make(map[int64]*biz.ETLStageExecution),
		nextTaskID:      1,
		nextExecID:      1,
		log:             log.NewHelper(logger),
	}

	// 初始化示例任务
	repo.initSampleTasks()
	return repo
}

// initSampleTasks 初始化示例ETL任务
func (r *etlTaskRepo) initSampleTasks() {
	now := time.Now()

	// Landing to ODS 任务
	task1 := &biz.ETLTask{
		ID:           1,
		Name:         "用户数据Landing到ODS",
		Description:  "将用户原始数据从Landing区迁移到ODS层",
		CurrentStage: biz.ETLStageLandingToODS,
		Status:       biz.ETLTaskStatusPending,
		Schedule:     "0 2 * * *", // 每天凌晨2点
		Config:       `{"source_table":"landing.user_raw","target_table":"ods.user_info","batch_size":10000}`,
		MaxRetries:   3,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	r.tasks[1] = task1
	r.nextTaskID = 2

	// ODS 任务
	task2 := &biz.ETLTask{
		ID:           2,
		Name:         "ODS层数据清洗",
		Description:  "对ODS层用户数据进行清洗和标准化",
		CurrentStage: biz.ETLStageODS,
		Status:       biz.ETLTaskStatusPending,
		Schedule:     "0 3 * * *", // 每天凌晨3点
		Config:       `{"source_table":"ods.user_info","target_table":"ods.user_clean","batch_size":5000,"validation_rules":["email_format","phone_format"]}`,
		MaxRetries:   3,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	r.tasks[2] = task2

	// DWD 任务
	task3 := &biz.ETLTask{
		ID:           3,
		Name:         "DWD层维度建模",
		Description:  "构建用户维度表和事实表",
		CurrentStage: biz.ETLStageDWD,
		Status:       biz.ETLTaskStatusPending,
		Schedule:     "0 4 * * *", // 每天凌晨4点
		Config:       `{"source_table":"ods.user_clean","target_table":"dwd.dim_user","batch_size":3000,"transform_rules":["user_segmentation","behavior_analysis"]}`,
		MaxRetries:   3,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	r.tasks[3] = task3

	// DWS 任务
	task4 := &biz.ETLTask{
		ID:           4,
		Name:         "DWS层数据汇总",
		Description:  "构建用户行为汇总表和指标表",
		CurrentStage: biz.ETLStageDWS,
		Status:       biz.ETLTaskStatusPending,
		Schedule:     "0 5 * * *", // 每天凌晨5点
		Config:       `{"source_table":"dwd.dim_user","target_table":"dws.user_summary","batch_size":2000,"aggregation_rules":["daily_active_users","user_retention"]}`,
		MaxRetries:   3,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	r.tasks[4] = task4

	// DS 任务
	task5 := &biz.ETLTask{
		ID:           5,
		Name:         "DS层应用数据集",
		Description:  "生成面向应用的数据集和报表",
		CurrentStage: biz.ETLStageDS,
		Status:       biz.ETLTaskStatusPending,
		Schedule:     "0 6 * * *", // 每天凌晨6点
		Config:       `{"source_table":"dws.user_summary","target_table":"ds.user_dashboard","batch_size":1000}`,
		MaxRetries:   3,
		RetryCount:   0,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	r.tasks[5] = task5
	r.nextTaskID = 6
}

// CreateTask 创建ETL任务
func (r *etlTaskRepo) CreateTask(ctx context.Context, task *biz.ETLTask) (*biz.ETLTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	task.ID = r.nextTaskID
	r.nextTaskID++
	task.CreatedAt = time.Now()
	task.UpdatedAt = task.CreatedAt
	r.tasks[task.ID] = task

	return task, nil
}

// UpdateTask 更新ETL任务
func (r *etlTaskRepo) UpdateTask(ctx context.Context, task *biz.ETLTask) (*biz.ETLTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[task.ID]; !exists {
		return nil, fmt.Errorf("task not found: %d", task.ID)
	}

	task.UpdatedAt = time.Now()
	r.tasks[task.ID] = task
	return task, nil
}

// DeleteTask 删除ETL任务
func (r *etlTaskRepo) DeleteTask(ctx context.Context, id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[id]; !exists {
		return fmt.Errorf("task not found: %d", id)
	}

	delete(r.tasks, id)
	return nil
}

// GetTask 获取ETL任务
func (r *etlTaskRepo) GetTask(ctx context.Context, id int64) (*biz.ETLTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	task, exists := r.tasks[id]
	if !exists {
		return nil, fmt.Errorf("task not found: %d", id)
	}

	return task, nil
}

// ListTasks 列出ETL任务
func (r *etlTaskRepo) ListTasks(ctx context.Context, status biz.ETLTaskStatus, limit, offset int) ([]*biz.ETLTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var tasks []*biz.ETLTask
	count := 0
	skipped := 0

	for _, task := range r.tasks {
		if status != "" && task.Status != status {
			continue
		}

		if skipped < offset {
			skipped++
			continue
		}

		if limit > 0 && count >= limit {
			break
		}

		tasks = append(tasks, task)
		count++
	}

	return tasks, nil
}

// GetTasksBySchedule 根据调度表达式获取任务
func (r *etlTaskRepo) GetTasksBySchedule(ctx context.Context, schedule string) ([]*biz.ETLTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var tasks []*biz.ETLTask
	for _, task := range r.tasks {
		if task.Schedule == schedule {
			tasks = append(tasks, task)
		}
	}

	return tasks, nil
}

// CreateStageExecution 创建阶段执行记录
func (r *etlTaskRepo) CreateStageExecution(ctx context.Context, execution *biz.ETLStageExecution) (*biz.ETLStageExecution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	execution.ID = r.nextExecID
	r.nextExecID++
	execution.StartedAt = time.Now()
	r.stageExecutions[execution.ID] = execution

	return execution, nil
}

// UpdateStageExecution 更新阶段执行记录
func (r *etlTaskRepo) UpdateStageExecution(ctx context.Context, execution *biz.ETLStageExecution) (*biz.ETLStageExecution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.stageExecutions[execution.ID]; !exists {
		return nil, fmt.Errorf("stage execution not found: %d", execution.ID)
	}

	r.stageExecutions[execution.ID] = execution
	return execution, nil
}

// GetStageExecutions 获取任务的阶段执行记录
func (r *etlTaskRepo) GetStageExecutions(ctx context.Context, taskID int64, limit int) ([]*biz.ETLStageExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var executions []*biz.ETLStageExecution
	count := 0

	for _, execution := range r.stageExecutions {
		if execution.TaskID == taskID {
			if limit > 0 && count >= limit {
				break
			}
			executions = append(executions, execution)
			count++
		}
	}

	return executions, nil
}

// GetLatestStageExecution 获取任务指定阶段的最新执行记录
func (r *etlTaskRepo) GetLatestStageExecution(ctx context.Context, taskID int64, stage biz.ETLStage) (*biz.ETLStageExecution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var latest *biz.ETLStageExecution

	for _, execution := range r.stageExecutions {
		if execution.TaskID == taskID && execution.Stage == stage {
			if latest == nil || execution.StartedAt.After(latest.StartedAt) {
				latest = execution
			}
		}
	}

	if latest == nil {
		return nil, fmt.Errorf("no stage execution found for task %d, stage %s", taskID, stage)
	}

	return latest, nil
}
