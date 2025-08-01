package data

import (
	"context"
	"fmt"
	"strconv"

	"ck/internal/biz"
	"ck/internal/data/model"
	"ck/internal/data/query"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

// orderDataRepo 订单数据仓储实现
type orderDataRepo struct {
	data *Data
	log  *log.Helper
}

// NewOrderDataRepo 创建订单数据仓储
func NewOrderDataRepo(data *Data, logger log.Logger) biz.OrderDataRepo {
	return &orderDataRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

// GetODSOrdersByCursor 使用游标分页获取ODS订单数据
func (r *orderDataRepo) GetODSOrdersByCursor(ctx context.Context, cursor string, limit int) ([]*biz.ODSOrder, string, error) {
	q := query.Use(r.data.db)
	odsOrderQuery := q.OdsOrder

	// 解析游标
	var lastID int64
	if cursor != "" {
		var err error
		lastID, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid cursor: %w", err)
		}
	}

	// 构建查询条件
	var odsOrders []*model.OdsOrder
	var err error

	if lastID > 0 {
		// 使用游标进行分页
		odsOrders, err = odsOrderQuery.WithContext(ctx).
			Where(odsOrderQuery.ID.Gt(lastID)).
			Order(odsOrderQuery.ID).
			Limit(limit).
			Find()
	} else {
		// 首次查询
		odsOrders, err = odsOrderQuery.WithContext(ctx).
			Order(odsOrderQuery.ID).
			Limit(limit).
			Find()
	}

	if err != nil {
		return nil, "", fmt.Errorf("failed to query ods orders: %w", err)
	}

	// 转换为业务实体
	bizOrders := make([]*biz.ODSOrder, len(odsOrders))
	for i, order := range odsOrders {
		bizOrders[i] = &biz.ODSOrder{
			ID:        order.ID,
			OrderID:   strconv.FormatInt(order.OrderID, 10),
			Detail:    order.Detail,
			CreatedAt: order.CreatedAt,
			UpdatedAt: order.UpdatedAt,
		}
	}

	// 计算下一个游标
	var nextCursor string
	if len(odsOrders) > 0 {
		lastOrder := odsOrders[len(odsOrders)-1]
		nextCursor = strconv.FormatInt(lastOrder.ID, 10)
	}

	return bizOrders, nextCursor, nil
}

// BatchInsertDWDOrders 批量插入DWD订单数据
func (r *orderDataRepo) BatchInsertDWDOrders(ctx context.Context, orders []*biz.DWDOrder) error {
	if len(orders) == 0 {
		return nil
	}

	q := query.Use(r.data.db)
	dwdOrderQuery := q.DwdOrder

	// 转换为数据模型
	dwdOrders := make([]*model.DwdOrder, len(orders))
	for i, order := range orders {
		dwdOrders[i] = &model.DwdOrder{
			OrderID:   order.OrderID,
			Detail:    order.Detail,
			CreatedAt: &order.CreatedAt,
			UpdatedAt: &order.UpdatedAt,
		}
	}

	// 批量插入
	err := dwdOrderQuery.WithContext(ctx).CreateInBatches(dwdOrders, 100)
	if err != nil {
		return fmt.Errorf("failed to batch insert dwd orders: %w", err)
	}

	r.log.Infof("Successfully inserted %d DWD orders", len(orders))
	return nil
}

// GetDWDOrderByOrderID 根据订单ID获取DWD订单
func (r *orderDataRepo) GetDWDOrderByOrderID(ctx context.Context, orderID string) (*biz.DWDOrder, error) {
	q := query.Use(r.data.db)
	dwdOrderQuery := q.DwdOrder

	order, err := dwdOrderQuery.WithContext(ctx).
		Where(dwdOrderQuery.OrderID.Eq(orderID)).
		First()

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get dwd order by order_id %s: %w", orderID, err)
	}

	// 转换为业务实体
	bizOrder := &biz.DWDOrder{
		ID:        order.ID,
		OrderID:   order.OrderID,
		Detail:    order.Detail,
		CreatedAt: *order.CreatedAt,
		UpdatedAt: *order.UpdatedAt,
	}

	return bizOrder, nil
}

// GetDWDOrdersByCursor 使用游标分页获取DWD订单数据
func (r *orderDataRepo) GetDWDOrdersByCursor(ctx context.Context, cursor string, limit int) ([]*biz.DWDOrder, string, error) {
	q := query.Use(r.data.db)
	dwdOrderQuery := q.DwdOrder

	// 构建查询条件
	query := dwdOrderQuery.WithContext(ctx)
	if cursor != "" {
		cursorID, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid cursor: %w", err)
		}
		query = query.Where(dwdOrderQuery.ID.Gt(cursorID))
	}

	// 执行查询
	dwdOrders, err := query.Order(dwdOrderQuery.ID).Limit(limit).Find()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get dwd orders by cursor: %w", err)
	}

	// 转换为业务实体
	bizOrders := make([]*biz.DWDOrder, len(dwdOrders))
	for i, order := range dwdOrders {
		bizOrders[i] = &biz.DWDOrder{
			ID:        order.ID,
			OrderID:   order.OrderID,
			Detail:    order.Detail,
			CreatedAt: *order.CreatedAt,
			UpdatedAt: *order.UpdatedAt,
		}
	}

	// 计算下一个游标
	var nextCursor string
	if len(dwdOrders) > 0 {
		lastOrder := dwdOrders[len(dwdOrders)-1]
		nextCursor = strconv.FormatInt(lastOrder.ID, 10)
	}

	return bizOrders, nextCursor, nil
}

// BatchInsertDWSOrders 批量插入DWS订单数据
func (r *orderDataRepo) BatchInsertDWSOrders(ctx context.Context, orders []*biz.DWSOrder) error {
	if len(orders) == 0 {
		return nil
	}

	q := query.Use(r.data.db)
	dwsOrderQuery := q.DwsOrder

	// 转换为数据模型
	dwsOrders := make([]*model.DwsOrder, len(orders))
	for i, order := range orders {
		dwsOrders[i] = &model.DwsOrder{
			OrderID:         order.OrderID,
			CustomerID:      order.CustomerID,
			OrderAmount:     order.OrderAmount,
			OrderStatus:     order.OrderStatus,
			OrderDate:       order.OrderDate,
			PaymentMethod:   order.PaymentMethod,
			ShippingAddress: order.ShippingAddress,
			PushStatus:      &order.PushStatus,
			CreatedAt:       &order.CreatedAt,
			UpdatedAt:       &order.UpdatedAt,
		}
	}

	// 批量插入
	err := dwsOrderQuery.WithContext(ctx).CreateInBatches(dwsOrders, 100)
	if err != nil {
		return fmt.Errorf("failed to batch insert dws orders: %w", err)
	}

	r.log.Infof("Successfully inserted %d DWS orders", len(orders))
	return nil
}

// GetDWSOrderByOrderID 根据订单ID获取DWS订单
func (r *orderDataRepo) GetDWSOrderByOrderID(ctx context.Context, orderID string) (*biz.DWSOrder, error) {
	q := query.Use(r.data.db)
	dwsOrderQuery := q.DwsOrder

	order, err := dwsOrderQuery.WithContext(ctx).Where(dwsOrderQuery.OrderID.Eq(orderID)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get dws order by order_id %s: %w", orderID, err)
	}

	// 转换为业务实体
	bizOrder := &biz.DWSOrder{
		ID:              order.ID,
		OrderID:         order.OrderID,
		CustomerID:      order.CustomerID,
		OrderAmount:     order.OrderAmount,
		OrderStatus:     order.OrderStatus,
		OrderDate:       order.OrderDate,
		PaymentMethod:   order.PaymentMethod,
		ShippingAddress: order.ShippingAddress,
		PushStatus:      *order.PushStatus,
		CreatedAt:       *order.CreatedAt,
		UpdatedAt:       *order.UpdatedAt,
	}

	return bizOrder, nil
}

// UpdateDWSOrderPushStatus 更新DWS订单推送状态
func (r *orderDataRepo) UpdateDWSOrderPushStatus(ctx context.Context, orderID string, status string) error {
	q := query.Use(r.data.db)
	dwsOrderQuery := q.DwsOrder

	_, err := dwsOrderQuery.WithContext(ctx).Where(dwsOrderQuery.OrderID.Eq(orderID)).UpdateSimple(dwsOrderQuery.PushStatus.Value(status))
	if err != nil {
		return fmt.Errorf("failed to update dws order push status for order_id %s: %w", orderID, err)
	}

	r.log.Infof("Updated DWS order %s push status to %s", orderID, status)
	return nil
}
