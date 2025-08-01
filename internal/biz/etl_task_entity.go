package biz

import (
	"time"

	"github.com/looplab/fsm"
)

// ETLStage ETL阶段枚举
type ETLStage string

const (
	ETLStageLandingToODS ETLStage = "landing_to_ods" // 从landing区到ODS
	ETLStageODS          ETLStage = "ods"            // ODS层处理
	ETLStageDWD          ETLStage = "dwd"            // DWD层处理
	ETLStageDWS          ETLStage = "dws"            // DWS层处理
	ETLStageDS           ETLStage = "ds"             // DS层处理
	ETLStageCompleted    ETLStage = "completed"      // 全部完成
)

// ETLTaskStatus ETL任务状态
type ETLTaskStatus string

const (
	ETLTaskStatusPending   ETLTaskStatus = "pending"   // 等待中
	ETLTaskStatusRunning   ETLTaskStatus = "running"   // 运行中
	ETLTaskStatusPaused    ETLTaskStatus = "paused"    // 暂停
	ETLTaskStatusCompleted ETLTaskStatus = "completed" // 完成
	ETLTaskStatusFailed    ETLTaskStatus = "failed"    // 失败
	ETLTaskStatusStopped   ETLTaskStatus = "stopped"   // 已停止
)

// ETLEvent ETL事件
type ETLEvent string

const (
	ETLEventStart         ETLEvent = "start"          // 开始任务
	ETLEventPause         ETLEvent = "pause"          // 暂停任务
	ETLEventResume        ETLEvent = "resume"         // 恢复任务
	ETLEventStageComplete ETLEvent = "stage_complete" // 阶段完成
	ETLEventFail          ETLEvent = "fail"           // 任务失败
	ETLEventReset         ETLEvent = "reset"          // 重置任务
)

// ETLTask ETL任务实体
type ETLTask struct {
	ID           int64         `json:"id"`
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	Schedule     string        `json:"schedule"`      // cron表达式
	Config       string        `json:"config"`        // JSON格式的配置
	Status       ETLTaskStatus `json:"status"`        // 任务状态
	CurrentStage ETLStage      `json:"current_stage"` // 当前阶段
	Cursor       string        `json:"cursor"`        // 游标位置，用于断点续传
	CreatedAt    time.Time     `json:"created_at"`
	UpdatedAt    time.Time     `json:"updated_at"`
	StartedAt    *time.Time    `json:"started_at,omitempty"`
	CompletedAt  *time.Time    `json:"completed_at,omitempty"`
	ErrorMessage string        `json:"error_message,omitempty"`
	RetryCount   int           `json:"retry_count"`
	MaxRetries   int           `json:"max_retries"`

	// 状态机实例（不序列化）
	FSM *fsm.FSM `json:"-"`
}

// ETLStageExecution ETL阶段执行记录
type ETLStageExecution struct {
	ID        int64      `json:"id"`
	TaskID    int64      `json:"task_id"`
	Stage     ETLStage   `json:"stage"`
	Status    string     `json:"status"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	ErrorMsg  string     `json:"error_msg,omitempty"`
	Metadata  string     `json:"metadata,omitempty"` // JSON格式的元数据
}

// ETLTaskConfig ETL任务配置
type ETLTaskConfig struct {
	SourceTable      string            `json:"source_table"`
	TargetTable      string            `json:"target_table"`
	BatchSize        int               `json:"batch_size"`
	ValidationRules  []string          `json:"validation_rules,omitempty"`
	TransformRules   []string          `json:"transform_rules,omitempty"`
	AggregationRules []string          `json:"aggregation_rules,omitempty"`
	CustomParams     map[string]string `json:"custom_params,omitempty"`
}
