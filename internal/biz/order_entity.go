package biz

import (
	"encoding/json"
	"time"
)

// ODSOrder ODS层订单实体
type ODSOrder struct {
	ID        int64     `json:"id"`
	OrderID   string    `json:"order_id"`
	Detail    string    `json:"detail"` // JSON格式的订单详情
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GetDetailAsMap 将Detail字段解析为map
func (o *ODSOrder) GetDetailAsMap() (map[string]interface{}, error) {
	var detail map[string]interface{}
	err := json.Unmarshal([]byte(o.Detail), &detail)
	return detail, err
}

// DWDOrder DWD层订单实体（已完成订单）
type DWDOrder struct {
	ID        int64     `json:"id"`
	OrderID   string    `json:"order_id"`
	Detail    string    `json:"detail"` // JSON格式的订单详情
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GetDetailAsMap 将Detail字段解析为map
func (d *DWDOrder) GetDetailAsMap() (map[string]interface{}, error) {
	var detail map[string]interface{}
	err := json.Unmarshal([]byte(d.Detail), &detail)
	return detail, err
}

// IsCompleted 检查订单是否已完成
func (d *DWDOrder) IsCompleted() bool {
	detail, err := d.GetDetailAsMap()
	if err != nil {
		return false
	}
	status, ok := detail["status"].(string)
	return ok && status == "completed"
}

// DWSOrder DWS层订单实体（服务层，拆分字段存储）
type DWSOrder struct {
	ID              int64      `json:"id"`
	OrderID         string     `json:"order_id"`
	CustomerID      *int64     `json:"customer_id"`
	OrderAmount     *float64   `json:"order_amount"`
	OrderStatus     *string    `json:"order_status"`
	OrderDate       *time.Time `json:"order_date"`
	PaymentMethod   *string    `json:"payment_method"`
	ShippingAddress *string    `json:"shipping_address"`
	PushStatus      string     `json:"push_status"` // pending, pushed, failed
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// OrderTransformer 订单数据转换器
type OrderTransformer struct{}

// TransformODSToDWD 将ODS订单转换为DWD订单（仅处理已完成订单）
func (t *OrderTransformer) TransformODSToDWD(odsOrder *ODSOrder) (*DWDOrder, error) {
	detail, err := odsOrder.GetDetailAsMap()
	if err != nil {
		return nil, err
	}

	// 检查订单状态，只处理已完成的订单
	status, ok := detail["status"].(string)
	if !ok || status != "completed" {
		return nil, nil // 不是已完成订单，返回nil
	}

	// 创建DWD订单，直接复制详情JSON
	dwdOrder := &DWDOrder{
		OrderID:   odsOrder.OrderID,
		Detail:    odsOrder.Detail, // 直接复制JSON详情
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	return dwdOrder, nil
}

// TransformDWDToDWS 将DWD订单转换为DWS订单（解析JSON字段）
func (t *OrderTransformer) TransformDWDToDWS(dwdOrder *DWDOrder) (*DWSOrder, error) {
	detail, err := dwdOrder.GetDetailAsMap()
	if err != nil {
		return nil, err
	}

	dwsOrder := &DWSOrder{
		OrderID:    dwdOrder.OrderID,
		PushStatus: "pending", // 默认推送状态为待推送
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	// 解析JSON字段到具体字段
	if customerID, ok := detail["customer_id"].(float64); ok {
		cid := int64(customerID)
		dwsOrder.CustomerID = &cid
	}

	if orderAmount, ok := detail["order_amount"].(float64); ok {
		dwsOrder.OrderAmount = &orderAmount
	}

	if orderStatus, ok := detail["status"].(string); ok {
		dwsOrder.OrderStatus = &orderStatus
	}

	if orderDateStr, ok := detail["order_date"].(string); ok {
		if orderDate, err := time.Parse("2006-01-02 15:04:05", orderDateStr); err == nil {
			dwsOrder.OrderDate = &orderDate
		}
	}

	if paymentMethod, ok := detail["payment_method"].(string); ok {
		dwsOrder.PaymentMethod = &paymentMethod
	}

	if shippingAddress, ok := detail["shipping_address"].(string); ok {
		dwsOrder.ShippingAddress = &shippingAddress
	}

	return dwsOrder, nil
}
