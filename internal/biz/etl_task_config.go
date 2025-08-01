package biz

import (
	"encoding/json"
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
)

// ETLTaskConfigManager ETL任务配置管理器
type ETLTaskConfigManager struct {
	log *log.Helper
}

// NewETLTaskConfigManager 创建配置管理器
func NewETLTaskConfigManager(logger log.Logger) *ETLTaskConfigManager {
	return &ETLTaskConfigManager{
		log: log.NewHelper(logger),
	}
}

// ParseConfig 解析任务配置
func (m *ETLTaskConfigManager) ParseConfig(configStr string) (*ETLTaskConfig, error) {
	if configStr == "" {
		return &ETLTaskConfig{}, nil
	}

	var config ETLTaskConfig
	if err := json.Unmarshal([]byte(configStr), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

// SerializeConfig 序列化任务配置
func (m *ETLTaskConfigManager) SerializeConfig(config *ETLTaskConfig) (string, error) {
	if config == nil {
		return "", nil
	}

	data, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("failed to serialize config: %w", err)
	}

	return string(data), nil
}

// ValidateConfig 验证配置的有效性
func (m *ETLTaskConfigManager) ValidateConfig(config *ETLTaskConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	if config.SourceTable == "" {
		return fmt.Errorf("source_table is required")
	}

	if config.TargetTable == "" {
		return fmt.Errorf("target_table is required")
	}

	if config.BatchSize <= 0 {
		config.BatchSize = 1000 // 设置默认值
	}

	return nil
}

// MergeConfig 合并配置
func (m *ETLTaskConfigManager) MergeConfig(base, override *ETLTaskConfig) *ETLTaskConfig {
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}

	result := *base // 复制基础配置

	// 覆盖非空字段
	if override.SourceTable != "" {
		result.SourceTable = override.SourceTable
	}
	if override.TargetTable != "" {
		result.TargetTable = override.TargetTable
	}
	if override.BatchSize > 0 {
		result.BatchSize = override.BatchSize
	}
	if len(override.ValidationRules) > 0 {
		result.ValidationRules = override.ValidationRules
	}
	if len(override.TransformRules) > 0 {
		result.TransformRules = override.TransformRules
	}
	if len(override.AggregationRules) > 0 {
		result.AggregationRules = override.AggregationRules
	}
	if len(override.CustomParams) > 0 {
		if result.CustomParams == nil {
			result.CustomParams = make(map[string]string)
		}
		for k, v := range override.CustomParams {
			result.CustomParams[k] = v
		}
	}

	return &result
}
