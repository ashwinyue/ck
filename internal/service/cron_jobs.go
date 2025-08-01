package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ck/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"
)

// CronJobManager 定时任务管理器
type CronJobManager struct {
	cronService    *CronService
	greeterUsecase *biz.GreeterUsecase
	etlUsecase     *biz.ETLTaskUsecase
	etlExecutor    *ETLExecutor
	log            *log.Helper
}

// NewCronJobManager 创建定时任务管理器
func NewCronJobManager(cs *CronService, gu *biz.GreeterUsecase, etlUsecase *biz.ETLTaskUsecase, etlExecutor *ETLExecutor, logger log.Logger) *CronJobManager {
	return &CronJobManager{
		cronService:    cs,
		greeterUsecase: gu,
		etlUsecase:     etlUsecase,
		etlExecutor:    etlExecutor,
		log:            log.NewHelper(logger),
	}
}

// RegisterBusinessJobs 注册业务相关的定时任务
func (m *CronJobManager) RegisterBusinessJobs() error {
	// 每5分钟执行一次数据同步任务
	_, err := m.cronService.AddJob("0 */5 * * * *", m.dataSyncJob)
	if err != nil {
		m.log.Errorf("Failed to register data sync job: %v", err)
		return err
	}

	// 每天凌晨3点执行数据备份任务
	_, err = m.cronService.AddJob("0 0 3 * * *", m.dataBackupJob)
	if err != nil {
		m.log.Errorf("Failed to register data backup job: %v", err)
		return err
	}

	// 每小时执行一次缓存清理任务
	_, err = m.cronService.AddJob("0 0 * * * *", m.cacheCleanupJob)
	if err != nil {
		m.log.Errorf("Failed to register cache cleanup job: %v", err)
		return err
	}

	// 每30秒执行一次健康检查任务
	_, err = m.cronService.AddJob("*/30 * * * * *", m.healthCheckJob)
	if err != nil {
		m.log.Errorf("Failed to register health check job: %v", err)
		return err
	}

	m.log.Info("Business cron jobs registered successfully")
	return nil
}

// dataSyncJob 数据同步任务
func (m *CronJobManager) dataSyncJob() {
	m.log.Info("Starting data sync job")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 示例：获取所有 greeter 数据进行同步
	greeters, err := m.greeterUsecase.ListAll(ctx)
	if err != nil {
		m.log.Errorf("Failed to list greeters for sync: %v", err)
		return
	}

	m.log.Infof("Data sync completed, processed %d greeters", len(greeters))
}

// dataBackupJob 数据备份任务
func (m *CronJobManager) dataBackupJob() {
	m.log.Info("Starting data backup job")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// 示例：备份所有重要数据
	greeters, err := m.greeterUsecase.ListAll(ctx)
	if err != nil {
		m.log.Errorf("Failed to list greeters for backup: %v", err)
		return
	}

	// 将数据序列化为 JSON 格式（实际场景中可能需要写入文件或上传到云存储）
	backupData, err := json.Marshal(greeters)
	if err != nil {
		m.log.Errorf("Failed to marshal backup data: %v", err)
		return
	}

	m.log.Infof("Data backup completed, backup size: %d bytes", len(backupData))
}

// cacheCleanupJob 缓存清理任务
func (m *CronJobManager) cacheCleanupJob() {
	m.log.Info("Starting cache cleanup job")

	// 这里可以添加具体的缓存清理逻辑
	// 例如：清理过期的 Redis 缓存、清理临时文件等

	m.log.Info("Cache cleanup completed")
}

// healthCheckJob 健康检查任务
func (m *CronJobManager) healthCheckJob() {
	m.log.Debug("Performing health check")

	// 检查数据库连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 尝试执行一个简单的查询来检查数据库健康状态
	_, err := m.greeterUsecase.ListAll(ctx)
	if err != nil {
		m.log.Errorf("Health check failed - database error: %v", err)
		return
	}

	m.log.Debug("Health check passed")
}

// GetJobStatus 获取定时任务状态
func (m *CronJobManager) GetJobStatus() map[string]interface{} {
	entries := m.cronService.GetEntries()
	status := make(map[string]interface{})

	status["total_jobs"] = len(entries)
	status["jobs"] = make([]map[string]interface{}, 0, len(entries))

	for _, entry := range entries {
		jobInfo := map[string]interface{}{
			"id":       entry.ID,
			"next_run": entry.Next,
			"prev_run": entry.Prev,
		}
		status["jobs"] = append(status["jobs"].([]map[string]interface{}), jobInfo)
	}

	return status
}

// AddCustomJob 添加自定义定时任务
func (m *CronJobManager) AddCustomJob(spec string, jobFunc func()) (cron.EntryID, error) {
	m.log.Infof("Adding custom job with spec: %s", spec)
	return m.cronService.AddJob(spec, jobFunc)
}

// RemoveCustomJob 移除自定义定时任务
func (m *CronJobManager) RemoveCustomJob(id cron.EntryID) {
	m.log.Infof("Removing custom job with ID: %d", id)
	m.cronService.RemoveJob(id)
}

// ETLTaskScanner ETL任务扫描器
func (m *CronJobManager) ETLTaskScanner() {
	ctx := context.Background()
	m.log.Info("Running ETL task scanner")

	// 获取待执行的ETL任务
	pendingTasks, err := m.etlUsecase.ListTasks(ctx, biz.ETLTaskStatusPending, 100, 0)
	if err != nil {
		m.log.Errorf("Failed to get pending ETL tasks: %v", err)
		return
	}

	if len(pendingTasks) == 0 {
		m.log.Debug("No pending ETL tasks found")
		return
	}

	m.log.Infof("Found %d pending ETL tasks", len(pendingTasks))

	// 检查每个任务的调度时间
	for _, task := range pendingTasks {
		if m.shouldExecuteTask(task) {
			m.log.Infof("Task %d (%s) is ready for execution", task.ID, task.Name)
			// 任务将由ETLExecutor的执行循环处理
		}
	}
}

// shouldExecuteTask 检查任务是否应该执行
func (m *CronJobManager) shouldExecuteTask(task *biz.ETLTask) bool {
	// 这里可以实现更复杂的调度逻辑
	// 例如：解析cron表达式，检查执行时间窗口等

	// 简单示例：检查任务是否有调度配置
	if task.Schedule == "" {
		return true // 没有调度配置，立即执行
	}

	// 这里应该解析cron表达式并检查是否到了执行时间
	// 为了简化，这里总是返回true
	return true
}

// ETLTaskMonitor ETL任务监控
func (m *CronJobManager) ETLTaskMonitor() {
	ctx := context.Background()
	m.log.Info("Running ETL task monitor")

	// 获取正在运行的任务
	runningTasks, err := m.etlUsecase.ListTasks(ctx, biz.ETLTaskStatusRunning, 100, 0)
	if err != nil {
		m.log.Errorf("Failed to get running tasks: %v", err)
		return
	}

	m.log.Infof("Currently running %d ETL tasks", len(runningTasks))

	// 检查长时间运行的任务
	for _, task := range runningTasks {
		// 获取最新的阶段执行记录
		executions, err := m.etlUsecase.GetStageExecutions(ctx, task.ID, 1)
		if err != nil {
			m.log.Errorf("Failed to get stage executions for task %d: %v", task.ID, err)
			continue
		}

		if len(executions) > 0 {
			execution := executions[0]
			duration := time.Since(execution.StartedAt)
			if duration > 30*time.Minute { // 超过30分钟的任务
				m.log.Warnf("Task %d has been running for %v", task.ID, duration)
				// 可以选择发送告警或采取其他措施
			}
		}
	}

	m.log.Info("ETL task monitoring completed")
}

// ETLTaskCleanup ETL任务清理
func (m *CronJobManager) ETLTaskCleanup() {
	m.log.Info("Running ETL task cleanup")

	// 这里可以实现清理逻辑
	// 例如：清理过期的执行记录、日志文件等
	m.log.Info("ETL task cleanup completed")
}

// DailyETLReport 每日ETL报告
func (m *CronJobManager) DailyETLReport() {
	ctx := context.Background()
	m.log.Info("Generating daily ETL report")

	// 获取各状态的任务统计
	pendingTasks, _ := m.etlUsecase.ListTasks(ctx, biz.ETLTaskStatusPending, 0, 0)
	runningTasks, _ := m.etlUsecase.ListTasks(ctx, biz.ETLTaskStatusRunning, 0, 0)
	completedTasks, _ := m.etlUsecase.ListTasks(ctx, biz.ETLTaskStatusCompleted, 0, 0)
	failedTasks, _ := m.etlUsecase.ListTasks(ctx, biz.ETLTaskStatusFailed, 0, 0)

	// 生成报告
	report := fmt.Sprintf("Daily ETL Report - %s\n", time.Now().Format("2006-01-02"))
	report += fmt.Sprintf("Completed tasks: %d\n", len(completedTasks))
	report += fmt.Sprintf("Failed tasks: %d\n", len(failedTasks))
	report += fmt.Sprintf("Pending tasks: %d\n", len(pendingTasks))
	report += fmt.Sprintf("Running tasks: %d\n", len(runningTasks))

	m.log.Infof("Daily ETL Report:\n%s", report)

	// 这里可以发送邮件、写入文件或发送到监控系统
}
