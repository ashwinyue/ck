package service

import (
	"context"
	"time"

	"ck/internal/conf"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/robfig/cron/v3"
)

// CronService 定时任务服务
type CronService struct {
	cron   *cron.Cron
	config *conf.Data_Cron
	log    *log.Helper
}

// NewCronService 创建定时任务服务
func NewCronService(c *conf.Data, logger log.Logger) *CronService {
	cronConf := c.Cron
	if !cronConf.Enabled {
		return &CronService{
			config: cronConf,
			log:    log.NewHelper(logger),
		}
	}

	// 设置时区
	location, err := time.LoadLocation(cronConf.Timezone)
	if err != nil {
		location = time.UTC
		log.NewHelper(logger).Warnf("Failed to load timezone %s, using UTC", cronConf.Timezone)
	}

	// 创建 cron 实例，使用秒级精度
	cronInstance := cron.New(
		cron.WithLocation(location),
		cron.WithSeconds(), // 支持秒级定时任务
	)

	cs := &CronService{
		cron:   cronInstance,
		config: cronConf,
		log:    log.NewHelper(logger),
	}

	// 注册默认定时任务
	cs.registerJobs()

	return cs
}

// Start 启动定时任务服务
func (s *CronService) Start(ctx context.Context) error {
	if s.cron == nil {
		s.log.Info("Cron service is disabled")
		return nil
	}

	s.log.Info("Starting cron service")
	s.cron.Start()
	return nil
}

// Stop 停止定时任务服务
func (s *CronService) Stop(ctx context.Context) error {
	if s.cron == nil {
		return nil
	}

	s.log.Info("Stopping cron service")
	ctx = s.cron.Stop()
	<-ctx.Done()
	s.log.Info("Cron service stopped")
	return nil
}

// AddJob 添加定时任务
func (s *CronService) AddJob(spec string, cmd func()) (cron.EntryID, error) {
	if s.cron == nil {
		return 0, nil
	}
	return s.cron.AddFunc(spec, cmd)
}

// RemoveJob 移除定时任务
func (s *CronService) RemoveJob(id cron.EntryID) {
	if s.cron == nil {
		return
	}
	s.cron.Remove(id)
}

// GetEntries 获取所有定时任务
func (s *CronService) GetEntries() []cron.Entry {
	if s.cron == nil {
		return nil
	}
	return s.cron.Entries()
}

// registerJobs 注册默认定时任务
func (s *CronService) registerJobs() {
	// 示例：每分钟执行一次的健康检查任务
	_, err := s.AddJob("0 * * * * *", func() {
		s.log.Info("Health check job executed")
		// 这里可以添加具体的健康检查逻辑
	})
	if err != nil {
		s.log.Errorf("Failed to add health check job: %v", err)
	}

	// 示例：每天凌晨2点执行的数据清理任务
	_, err = s.AddJob("0 0 2 * * *", func() {
		s.log.Info("Data cleanup job executed")
		// 这里可以添加具体的数据清理逻辑
	})
	if err != nil {
		s.log.Errorf("Failed to add data cleanup job: %v", err)
	}

	// 示例：每小时执行一次的统计任务
	_, err = s.AddJob("0 0 * * * *", func() {
		s.log.Info("Statistics job executed")
		// 这里可以添加具体的统计逻辑
	})
	if err != nil {
		s.log.Errorf("Failed to add statistics job: %v", err)
	}

	s.log.Info("Default cron jobs registered")
}

// RegisterJobs 注册定时任务
func (s *CronService) RegisterJobs(manager *CronJobManager) {
	s.log.Info("Registering cron jobs")

	// 注册数据同步任务 - 每小时执行一次
	if _, err := s.cron.AddFunc("0 * * * *", manager.dataSyncJob); err != nil {
		s.log.Errorf("Failed to register data sync job: %v", err)
	}

	// 注册数据备份任务 - 每天凌晨2点执行
	if _, err := s.cron.AddFunc("0 2 * * *", manager.dataBackupJob); err != nil {
		s.log.Errorf("Failed to register data backup job: %v", err)
	}

	// 注册缓存清理任务 - 每天凌晨3点执行
	if _, err := s.cron.AddFunc("0 3 * * *", manager.cacheCleanupJob); err != nil {
		s.log.Errorf("Failed to register cache cleanup job: %v", err)
	}

	// 注册健康检查任务 - 每5分钟执行一次
	if _, err := s.cron.AddFunc("*/5 * * * *", manager.healthCheckJob); err != nil {
		s.log.Errorf("Failed to register health check job: %v", err)
	}

	// 注册ETL任务扫描器 - 每分钟执行一次
	if _, err := s.cron.AddFunc("* * * * *", manager.ETLTaskScanner); err != nil {
		s.log.Errorf("Failed to register ETL task scanner job: %v", err)
	}

	// 注册ETL任务监控 - 每5分钟执行一次
	if _, err := s.cron.AddFunc("*/5 * * * *", manager.ETLTaskMonitor); err != nil {
		s.log.Errorf("Failed to register ETL task monitor job: %v", err)
	}

	// 注册ETL任务清理 - 每天凌晨4点执行
	if _, err := s.cron.AddFunc("0 4 * * *", manager.ETLTaskCleanup); err != nil {
		s.log.Errorf("Failed to register ETL task cleanup job: %v", err)
	}

	// 注册每日ETL报告 - 每天早上8点执行
	if _, err := s.cron.AddFunc("0 8 * * *", manager.DailyETLReport); err != nil {
		s.log.Errorf("Failed to register daily ETL report job: %v", err)
	}

	s.log.Info("All cron jobs registered successfully")
}
