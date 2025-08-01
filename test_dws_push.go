package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DWSOrder DWS层订单实体
type DWSOrder struct {
	ID              int64      `json:"id"`
	OrderID         string     `json:"order_id"`
	CustomerID      *int64     `json:"customer_id"`
	OrderAmount     *float64   `json:"order_amount"`
	OrderStatus     *string    `json:"order_status"`
	OrderDate       *time.Time `json:"order_date"`
	PaymentMethod   *string    `json:"payment_method"`
	ShippingAddress *string    `json:"shipping_address"`
	PushStatus      string     `json:"push_status"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// CreateETLTaskRequest 创建ETL任务请求
type CreateETLTaskRequest struct {
	BatchSize int `json:"batch_size"`
}

// CreateETLTaskResponse 创建ETL任务响应
type CreateETLTaskResponse struct {
	TaskId string `json:"taskId"`
}

func main() {
	ctx := context.Background()

	// 1. 首先插入一些测试数据到DWD层
	fmt.Println("=== 插入测试数据到DWD层 ===")
	err := insertTestDWDData()
	if err != nil {
		log.Fatalf("Failed to insert test DWD data: %v", err)
	}

	// 2. 创建DWD到DWS的ETL任务
	fmt.Println("\n=== 创建DWD到DWS的ETL任务 ===")
	taskID, err := createDWDToDWSTask(ctx)
	if err != nil {
		log.Fatalf("Failed to create DWD to DWS task: %v", err)
	}
	fmt.Printf("Created ETL task with ID: %d\n", taskID)

	// 3. 启动ETL任务
	fmt.Println("\n=== 启动ETL任务 ===")
	err = startETLTask(ctx, taskID)
	if err != nil {
		log.Fatalf("Failed to start ETL task: %v", err)
	}
	fmt.Printf("Started ETL task %d\n", taskID)

	// 4. 等待任务完成
	fmt.Println("\n=== 等待任务完成 ===")
	time.Sleep(10 * time.Second)

	// 5. 检查任务状态
	fmt.Println("\n=== 检查任务状态 ===")
	err = checkTaskStatus(ctx, taskID)
	if err != nil {
		log.Fatalf("Failed to check task status: %v", err)
	}

	fmt.Println("\n=== 测试完成 ===")
	fmt.Println("请检查RocketMQ控制台 http://localhost:8080 查看消息推送情况")
}

func insertTestDWDData() error {
	// 这里应该直接插入到数据库，为了简化，我们假设数据已经存在
	fmt.Println("假设DWD层已有测试数据...")
	return nil
}

func createDWDToDWSTask(ctx context.Context) (int64, error) {
	reqBody := map[string]interface{}{
		"name":        "DWD to DWS ETL Task",
		"description": "Transform DWD orders to DWS orders",
		"schedule":    "manual",
		"start_stage": 3, // ETL_STAGE_DWD
		"end_stage":   4, // ETL_STAGE_DWS
		"config": map[string]string{
			"batch_size": "10",
		},
		"max_retries": 3,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post("http://localhost:8000/api/v1/etl/tasks", "application/json", strings.NewReader(string(jsonData)))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	// 读取响应内容并打印
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	fmt.Printf("Response body: %s\n", string(respBody))

	var result CreateETLTaskResponse
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		return 0, err
	}

	taskId, err := strconv.ParseInt(result.TaskId, 10, 64)
	if err != nil {
		return 0, err
	}
	return taskId, nil
}

func startETLTask(ctx context.Context, taskID int64) error {
	url := fmt.Sprintf("http://localhost:8000/api/v1/etl/tasks/%d/execute", taskID)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	return nil
}

func checkTaskStatus(ctx context.Context, taskID int64) error {
	url := fmt.Sprintf("http://localhost:8000/api/v1/etl/tasks/%d", taskID)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	var task map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&task)
	if err != nil {
		return err
	}

	fmt.Printf("Task Status: %v\n", task["status"])
	fmt.Printf("Current Stage: %v\n", task["current_stage"])
	return nil
}
