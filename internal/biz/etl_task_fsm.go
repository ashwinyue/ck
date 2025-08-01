package biz

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/looplab/fsm"
)

// ETLTaskFSMManager ETL任务状态机管理器
type ETLTaskFSMManager struct {
	log *log.Helper
}

// NewETLTaskFSMManager 创建状态机管理器
func NewETLTaskFSMManager(logger log.Logger) *ETLTaskFSMManager {
	return &ETLTaskFSMManager{
		log: log.NewHelper(logger),
	}
}

// CreateFSM 为任务创建状态机
func (m *ETLTaskFSMManager) CreateFSM(task *ETLTask) *fsm.FSM {
	return fsm.NewFSM(
		string(task.CurrentStage),
		fsm.Events{
			// 启动任务
			{Name: string(ETLEventStart), Src: []string{string(ETLStageLandingToODS)}, Dst: string(ETLTaskStatusRunning)},

			// 暂停任务
			{Name: string(ETLEventPause), Src: []string{string(ETLTaskStatusRunning)}, Dst: string(ETLTaskStatusPaused)},

			// 恢复任务
			{Name: string(ETLEventResume), Src: []string{string(ETLTaskStatusPaused)}, Dst: string(ETLTaskStatusRunning)},

			// 阶段完成转换
			{Name: string(ETLEventStageComplete), Src: []string{string(ETLStageLandingToODS)}, Dst: string(ETLStageODS)},
			{Name: string(ETLEventStageComplete), Src: []string{string(ETLStageODS)}, Dst: string(ETLStageDWD)},
			{Name: string(ETLEventStageComplete), Src: []string{string(ETLStageDWD)}, Dst: string(ETLStageDWS)},
			{Name: string(ETLEventStageComplete), Src: []string{string(ETLStageDWS)}, Dst: string(ETLStageDS)},
			{Name: string(ETLEventStageComplete), Src: []string{string(ETLStageDS)}, Dst: string(ETLStageCompleted)},

			// 任务失败
			{Name: string(ETLEventFail), Src: []string{
				string(ETLTaskStatusRunning),
				string(ETLStageLandingToODS),
				string(ETLStageODS),
				string(ETLStageDWD),
				string(ETLStageDWS),
				string(ETLStageDS),
			}, Dst: string(ETLTaskStatusFailed)},

			// 重置任务
			{Name: string(ETLEventReset), Src: []string{
				string(ETLTaskStatusFailed),
				string(ETLTaskStatusCompleted),
				string(ETLTaskStatusPaused),
			}, Dst: string(ETLStageLandingToODS)},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				m.log.Infof("Task %d: %s -> %s", task.ID, e.Src, e.Dst)
			},
		},
	)
}

// ValidateTransition 验证状态转换是否有效
func (m *ETLTaskFSMManager) ValidateTransition(task *ETLTask, event ETLEvent) error {
	if task.FSM == nil {
		task.FSM = m.CreateFSM(task)
	}
	return task.FSM.Event(context.Background(), string(event))
}

// GetCurrentState 获取当前状态
func (m *ETLTaskFSMManager) GetCurrentState(task *ETLTask) string {
	if task.FSM == nil {
		return string(task.CurrentStage)
	}
	return task.FSM.Current()
}

// GetAvailableTransitions 获取可用的状态转换
func (m *ETLTaskFSMManager) GetAvailableTransitions(task *ETLTask) []string {
	if task.FSM == nil {
		task.FSM = m.CreateFSM(task)
	}
	return task.FSM.AvailableTransitions()
}
