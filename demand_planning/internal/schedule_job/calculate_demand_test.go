package schedule_job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/config"
	mockFulfillmentStrategy "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/fulfillment_strategy"
	mockKafka "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/kafka"
	mockWHCentral "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/whcentral"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/provider"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/scheduler"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	whCentral "go.tekoapis.com/tekone/app/warehouse/central-service/api"
	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type jobCalculateDemandTestSuite struct {
	tests.TestSuite
	faker     *faker.Faker
	db        *gorm.DB
	handler   *scheduler.Handler
	ctx       context.Context
	fixedTime time.Time
}

type AssertObject struct {
	SiteSkuDemands          []*model.SiteSkuDemand
	SiteGroupDemands        []*model.SiteGroupDemand
	SiteSkuDailyForecasts   []*model.SiteSkuDailyForecast
	SiteGroupDailyForecasts []*model.SiteGroupDailyForecast
}

func TestJobCalculateDemand(t *testing.T) {
	ts := &jobCalculateDemandTestSuite{}
	db, _ := ts.InitIntegrationTestMain(t, gormDb, logService)
	ts.db = db
	ts.fixedTime = time.Date(2023, 11, 1, 0, 0, 0, 0, time.UTC)
	ts.faker = &faker.Faker{DB: db}

	defer func() {
		ts.tearDown()
		monkey.UnpatchAll()
	}()

	whCentralClient := &mockWHCentral.ClientAdapter{}
	whCentralClient.Mock.On("GetMapSiteId2SiteInfo", mock.Anything, mock.Anything, mock.Anything).
		Return(map[int32]*whCentral.Site{
			1: {
				Id:             1,
				LocationId:     "0101",
				SellerId:       1,
				SellerSiteCode: "SITE 1",
			},
			2: {
				Id:             2,
				LocationId:     "010101",
				SellerId:       1,
				SellerSiteCode: "SITE 2",
			},
		}, nil)
	whCentralClient.Mock.On("GetMapWarehouse2WarehouseInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]*whCentral.Warehouse{
			"WH1.1": {
				Code:     "WH1.1",
				SiteId:   1,
				Name:     "Warehouse 1",
				SellerId: 1,
			},
			"WH1.2": {
				Code:     "WH1.2",
				SiteId:   1,
				Name:     "Warehouse 2",
				SellerId: 1,
			},
			"WH2.1": {
				Code:     "WH2.1",
				SiteId:   2,
				Name:     "Warehouse 3",
				SellerId: 1,
			},
			"WH2.2": {
				Code:     "WH2.2",
				SiteId:   2,
				Name:     "Warehouse 4",
				SellerId: 1,
			},
		}, nil)

	fulfillmentStrategyClient := &mockFulfillmentStrategy.ClientAdapter{}
	fulfillmentStrategyClient.Mock.On("GetWarehousePriorityConfig", mock.Anything, mock.Anything).
		Return(map[string]map[string]int32{
			"01": {
				"WH2.1": 1,
				"WH2.2": 2,
			},
			"010101": {
				"WH2.1": 1,
				"WH1.2": 2,
			},
		}, nil)

	kafkaPublisher := &mockKafka.KafkaPublisher{}
	kafkaPublisher.Mock.On("PushKafkaEvent", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	monkey.Patch(time.Now, func() time.Time {
		return ts.fixedTime
	})

	ts.ctx = context.Background()
	ts.handler = scheduler.NewHandler(&service.Service{
		Config: &config.Config{
			SchedulerConfig: config.SchedulerConfig{
				DefaultMaxRetry: 0,
			},
		},
		SiteSkuDemandRepository:                repository.NewSiteSkuDemandRepository(db),
		SiteGroupDemandRepository:              repository.NewSiteGroupDemandRepository(db),
		MonthlyCategoryRepository:              repository.NewMonthlyCategoryRepository(db),
		MonthlySkuCategoryMappingRepository:    repository.NewMonthlySkuCategoryMappingRepository(db),
		MonthlySkuSegmentPathMappingRepository: repository.NewMonthlySkuSegmentPathMappingRepository(db),
		ScheduleJobRepository:                  repository.NewScheduleJobRepository(db),
		SiteSkuDailyForecastRepository:         repository.NewSiteSkuDailyForecastRepository(db),
		SiteGroupDailyForecastRepository:       repository.NewSiteGroupDailyForecastRepository(db),
		DB:                                     db,
		WHCentralClient:                        whCentralClient,
		FulfillmentStrategyServiceClient:       fulfillmentStrategyClient,
		KafkaPublisher:                         kafkaPublisher,
		ScheduleJobProvider:                    provider.NewScheduleJobProvider(db, nil),
	})

	suite.Run(t, ts)
}

func (ts *jobCalculateDemandTestSuite) setUp() {
	monthOfYear := "2023-11"

	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  1,
		Code:        "CATE1",
		Path:        helper.StringToSqlString("1"),
		Name:        "Category 1",
		MonthOfYear: monthOfYear,
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  2,
		Code:        "CATE2",
		Path:        helper.StringToSqlString("1/2"),
		Name:        "Category 2",
		ParentId:    helper.Int32ToSqlInt32(1),
		MonthOfYear: monthOfYear,
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  3,
		Code:        "CATE3",
		Path:        helper.StringToSqlString("1/3"),
		Name:        "Category 3",
		ParentId:    helper.Int32ToSqlInt32(1),
		MonthOfYear: monthOfYear,
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  22,
		Code:        "CATE22",
		Path:        helper.StringToSqlString("1/22"),
		Name:        "Category 22",
		MonthOfYear: monthOfYear,
	}, true)

	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku1",
		SellerSku:             "sellerSku1",
		Name:                  "sku name 1",
		LatestChildCategoryId: 2,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku2",
		SellerSku:             "sellerSku2",
		Name:                  "sku name 2",
		LatestChildCategoryId: 2,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku3",
		SellerSku:             "sellerSku3",
		Name:                  "sku name 3",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku4",
		SellerSku:             "sellerSku4",
		Name:                  "sku name 4",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku5",
		SellerSku:             "sellerSku5",
		Name:                  "sku name 5",
		LatestChildCategoryId: 22,
		MonthOfYear:           monthOfYear,
	}, true)

	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku1",
		MasterCategoryId: 1,
		SegmentPath:      "1/2",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku2",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku3",
		MasterCategoryId: 1,
		SegmentPath:      "1/2",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku4",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     20,
		SellerId:               1,
		Sku:                    "sku5",
		SiteId:                 2,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(100),
		Forecast:               helper.Float64ToSqlFloat64(1000),
		HandoverQty:            helper.Float64ToSqlFloat64(10),
		PickupQty:              helper.Float64ToSqlFloat64(10),
		AvgSellPrice:           helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(100),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(15),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Sku1
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     1,
		SellerId:               1,
		Sku:                    "sku1",
		SiteId:                 0,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(100),
		Forecast:               helper.Float64ToSqlFloat64(1000),
		HandoverQty:            helper.Float64ToSqlFloat64(10),
		PickupQty:              helper.Float64ToSqlFloat64(10),
		AvgSellPrice:           helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(100),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(15),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     2,
		SellerId:               1,
		Sku:                    "sku1",
		SiteId:                 1,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(40),
		Forecast:               helper.Float64ToSqlFloat64(400),
		HandoverQty:            helper.Float64ToSqlFloat64(4),
		PickupQty:              helper.Float64ToSqlFloat64(4),
		AvgSellPrice:           helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(40),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(400),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(15),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(400),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(400),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     3,
		SellerId:               1,
		Sku:                    "sku1",
		SiteId:                 2,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(60),
		Forecast:               helper.Float64ToSqlFloat64(600),
		HandoverQty:            helper.Float64ToSqlFloat64(6),
		PickupQty:              helper.Float64ToSqlFloat64(6),
		AvgSellPrice:           helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(60),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(600),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(15),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(600),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(600),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Sku2
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     4,
		SellerId:               1,
		Sku:                    "sku2",
		SiteId:                 0,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(100),
		Forecast:               helper.Float64ToSqlFloat64(1000),
		HandoverQty:            helper.Float64ToSqlFloat64(10),
		PickupQty:              helper.Float64ToSqlFloat64(10),
		AvgSellPrice:           helper.Float64ToSqlFloat64(200),
		ActualSaleValue:        helper.Float64ToSqlFloat64(100),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(150),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     5,
		SellerId:               1,
		Sku:                    "sku2",
		SiteId:                 1,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(50),
		Forecast:               helper.Float64ToSqlFloat64(500),
		HandoverQty:            helper.Float64ToSqlFloat64(5),
		PickupQty:              helper.Float64ToSqlFloat64(5),
		AvgSellPrice:           helper.Float64ToSqlFloat64(200),
		ActualSaleValue:        helper.Float64ToSqlFloat64(50),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(500),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(150),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(500),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(500),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     6,
		SellerId:               1,
		Sku:                    "sku2",
		SiteId:                 2,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(50),
		Forecast:               helper.Float64ToSqlFloat64(500),
		HandoverQty:            helper.Float64ToSqlFloat64(5),
		PickupQty:              helper.Float64ToSqlFloat64(5),
		AvgSellPrice:           helper.Float64ToSqlFloat64(200),
		ActualSaleValue:        helper.Float64ToSqlFloat64(50),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(500),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(150),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(500),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(500),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Sku3
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     7,
		SellerId:               1,
		Sku:                    "sku3",
		SiteId:                 0,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(1000),
		Forecast:               helper.Float64ToSqlFloat64(10000),
		HandoverQty:            helper.Float64ToSqlFloat64(100),
		PickupQty:              helper.Float64ToSqlFloat64(100),
		AvgSellPrice:           helper.Float64ToSqlFloat64(200),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1000),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(10000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(150),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(10000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(10000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     8,
		SellerId:               1,
		Sku:                    "sku3",
		SiteId:                 1,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(300),
		Forecast:               helper.Float64ToSqlFloat64(3000),
		HandoverQty:            helper.Float64ToSqlFloat64(30),
		PickupQty:              helper.Float64ToSqlFloat64(30),
		AvgSellPrice:           helper.Float64ToSqlFloat64(200),
		ActualSaleValue:        helper.Float64ToSqlFloat64(300),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(3000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(150),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(3000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(3000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     9,
		SellerId:               1,
		Sku:                    "sku3",
		SiteId:                 2,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(700),
		Forecast:               helper.Float64ToSqlFloat64(7000),
		HandoverQty:            helper.Float64ToSqlFloat64(70),
		PickupQty:              helper.Float64ToSqlFloat64(70),
		AvgSellPrice:           helper.Float64ToSqlFloat64(200),
		ActualSaleValue:        helper.Float64ToSqlFloat64(700),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(7000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(150),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(7000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(7000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Sku4
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     10,
		SellerId:               1,
		Sku:                    "sku4",
		SiteId:                 0,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(10000),
		Forecast:               helper.Float64ToSqlFloat64(100000),
		HandoverQty:            helper.Float64ToSqlFloat64(1000),
		PickupQty:              helper.Float64ToSqlFloat64(1000),
		AvgSellPrice:           helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(10000),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(100000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(15),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(100000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(100000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     11,
		SellerId:               1,
		Sku:                    "sku4",
		SiteId:                 1,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(8000),
		Forecast:               helper.Float64ToSqlFloat64(80000),
		HandoverQty:            helper.Float64ToSqlFloat64(800),
		PickupQty:              helper.Float64ToSqlFloat64(800),
		AvgSellPrice:           helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(8000),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(8000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(15),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(80000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(80000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:                     12,
		SellerId:               1,
		Sku:                    "sku4",
		SiteId:                 2,
		MonthOfYear:            monthOfYear,
		Budget:                 helper.Float64ToSqlFloat64(2000),
		Forecast:               helper.Float64ToSqlFloat64(20000),
		HandoverQty:            helper.Float64ToSqlFloat64(200),
		PickupQty:              helper.Float64ToSqlFloat64(200),
		AvgSellPrice:           helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(2000),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(2000),
		AvgPurchasePrice:       helper.Float64ToSqlFloat64(15),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(20000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(20000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)

	// Group by category, group key = 1
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     1,
		SellerId:               1,
		GroupKey:               "1",
		GroupBy:                "category",
		SiteId:                 0,
		NumberOfSkus:           1120,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(11200),
		Forecast:               helper.Float64ToSqlFloat64(112000),
		HandoverQty:            helper.Float64ToSqlFloat64(1120),
		PickupQty:              helper.Float64ToSqlFloat64(1120),
		ActualSaleValue:        helper.Float64ToSqlFloat64(11200),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(112000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(112000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(112000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     2,
		SellerId:               1,
		GroupKey:               "1",
		GroupBy:                "category",
		SiteId:                 1,
		NumberOfSkus:           839,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(8390),
		Forecast:               helper.Float64ToSqlFloat64(83900),
		HandoverQty:            helper.Float64ToSqlFloat64(839),
		PickupQty:              helper.Float64ToSqlFloat64(839),
		ActualSaleValue:        helper.Float64ToSqlFloat64(8390),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(83900),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(83900),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(83900),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     3,
		SellerId:               1,
		GroupKey:               "1",
		GroupBy:                "category",
		SiteId:                 2,
		NumberOfSkus:           281,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(2810),
		Forecast:               helper.Float64ToSqlFloat64(28100),
		HandoverQty:            helper.Float64ToSqlFloat64(281),
		PickupQty:              helper.Float64ToSqlFloat64(281),
		ActualSaleValue:        helper.Float64ToSqlFloat64(2810),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(28100),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(28100),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(28100),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Group by category, group key = 1/2
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     4,
		SellerId:               1,
		GroupKey:               "1/2",
		GroupBy:                "category",
		SiteId:                 0,
		NumberOfSkus:           200,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(200),
		Forecast:               helper.Float64ToSqlFloat64(2000),
		HandoverQty:            helper.Float64ToSqlFloat64(20),
		PickupQty:              helper.Float64ToSqlFloat64(20),
		ActualSaleValue:        helper.Float64ToSqlFloat64(200),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(2000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(2000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(2000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     5,
		SellerId:               1,
		GroupKey:               "1/2",
		GroupBy:                "category",
		SiteId:                 1,
		NumberOfSkus:           90,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(90),
		Forecast:               helper.Float64ToSqlFloat64(900),
		HandoverQty:            helper.Float64ToSqlFloat64(9),
		PickupQty:              helper.Float64ToSqlFloat64(9),
		ActualSaleValue:        helper.Float64ToSqlFloat64(90),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(900),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(900),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(900),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     6,
		SellerId:               1,
		GroupKey:               "1/2",
		GroupBy:                "category",
		SiteId:                 2,
		NumberOfSkus:           110,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(110),
		Forecast:               helper.Float64ToSqlFloat64(1100),
		HandoverQty:            helper.Float64ToSqlFloat64(11),
		PickupQty:              helper.Float64ToSqlFloat64(11),
		ActualSaleValue:        helper.Float64ToSqlFloat64(110),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1100),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1100),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1100),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Group by category, group key = 1/3
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     7,
		SellerId:               1,
		GroupKey:               "1/3",
		GroupBy:                "category",
		SiteId:                 0,
		NumberOfSkus:           1100,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(11000),
		Forecast:               helper.Float64ToSqlFloat64(110000),
		HandoverQty:            helper.Float64ToSqlFloat64(1100),
		PickupQty:              helper.Float64ToSqlFloat64(1100),
		ActualSaleValue:        helper.Float64ToSqlFloat64(11000),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(110000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(110000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(110000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     8,
		SellerId:               1,
		GroupKey:               "1/3",
		GroupBy:                "category",
		SiteId:                 1,
		NumberOfSkus:           830,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(8300),
		Forecast:               helper.Float64ToSqlFloat64(83000),
		HandoverQty:            helper.Float64ToSqlFloat64(830),
		PickupQty:              helper.Float64ToSqlFloat64(830),
		ActualSaleValue:        helper.Float64ToSqlFloat64(8300),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(83000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(83000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(83000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     9,
		SellerId:               1,
		GroupKey:               "1/3",
		GroupBy:                "category",
		SiteId:                 2,
		NumberOfSkus:           270,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(2700),
		Forecast:               helper.Float64ToSqlFloat64(27000),
		HandoverQty:            helper.Float64ToSqlFloat64(270),
		PickupQty:              helper.Float64ToSqlFloat64(270),
		ActualSaleValue:        helper.Float64ToSqlFloat64(2700),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(27000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(27000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(27000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Group by segment, group key = 1
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     10,
		SellerId:               1,
		GroupKey:               "1",
		GroupBy:                "segment",
		SiteId:                 0,
		NumberOfSkus:           1120,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(11200),
		Forecast:               helper.Float64ToSqlFloat64(112000),
		HandoverQty:            helper.Float64ToSqlFloat64(1120),
		PickupQty:              helper.Float64ToSqlFloat64(1120),
		ActualSaleValue:        helper.Float64ToSqlFloat64(11200),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(112000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(112000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(112000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     11,
		SellerId:               1,
		GroupKey:               "1",
		GroupBy:                "segment",
		SiteId:                 1,
		NumberOfSkus:           839,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(8390),
		Forecast:               helper.Float64ToSqlFloat64(83900),
		HandoverQty:            helper.Float64ToSqlFloat64(839),
		PickupQty:              helper.Float64ToSqlFloat64(839),
		ActualSaleValue:        helper.Float64ToSqlFloat64(8390),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(83900),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(83900),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(83900),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     12,
		SellerId:               1,
		GroupKey:               "1",
		GroupBy:                "segment",
		SiteId:                 2,
		NumberOfSkus:           281,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(2810),
		Forecast:               helper.Float64ToSqlFloat64(28100),
		HandoverQty:            helper.Float64ToSqlFloat64(281),
		PickupQty:              helper.Float64ToSqlFloat64(281),
		ActualSaleValue:        helper.Float64ToSqlFloat64(2810),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(28100),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(28100),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(28100),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Group by segment, group key = 1/1
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     13,
		SellerId:               1,
		GroupKey:               "1/1",
		GroupBy:                "segment",
		SiteId:                 0,
		NumberOfSkus:           1120,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(11200),
		Forecast:               helper.Float64ToSqlFloat64(112000),
		HandoverQty:            helper.Float64ToSqlFloat64(1120),
		PickupQty:              helper.Float64ToSqlFloat64(1120),
		ActualSaleValue:        helper.Float64ToSqlFloat64(11200),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(112000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(112000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(112000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     14,
		SellerId:               1,
		GroupKey:               "1/1",
		GroupBy:                "segment",
		SiteId:                 1,
		NumberOfSkus:           839,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(8390),
		Forecast:               helper.Float64ToSqlFloat64(83900),
		HandoverQty:            helper.Float64ToSqlFloat64(839),
		PickupQty:              helper.Float64ToSqlFloat64(839),
		ActualSaleValue:        helper.Float64ToSqlFloat64(8390),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(83900),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(83900),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(83900),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     15,
		SellerId:               1,
		GroupKey:               "1/1",
		GroupBy:                "segment",
		SiteId:                 2,
		NumberOfSkus:           281,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          false,
		Budget:                 helper.Float64ToSqlFloat64(2810),
		Forecast:               helper.Float64ToSqlFloat64(28100),
		HandoverQty:            helper.Float64ToSqlFloat64(281),
		PickupQty:              helper.Float64ToSqlFloat64(281),
		ActualSaleValue:        helper.Float64ToSqlFloat64(2810),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(28100),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(28100),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(28100),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Group by segment, group key = 1/1/2
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     16,
		SellerId:               1,
		GroupKey:               "1/1/2",
		GroupBy:                "segment",
		SiteId:                 0,
		NumberOfSkus:           1100,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(1100),
		Forecast:               helper.Float64ToSqlFloat64(11000),
		HandoverQty:            helper.Float64ToSqlFloat64(110),
		PickupQty:              helper.Float64ToSqlFloat64(110),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1100),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(11000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(11000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(11000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     17,
		SellerId:               1,
		GroupKey:               "1/1/2",
		GroupBy:                "segment",
		SiteId:                 1,
		NumberOfSkus:           340,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(340),
		Forecast:               helper.Float64ToSqlFloat64(3400),
		HandoverQty:            helper.Float64ToSqlFloat64(34),
		PickupQty:              helper.Float64ToSqlFloat64(34),
		ActualSaleValue:        helper.Float64ToSqlFloat64(340),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(3400),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(3400),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(3400),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     18,
		SellerId:               1,
		GroupKey:               "1/1/2",
		GroupBy:                "segment",
		SiteId:                 2,
		NumberOfSkus:           760,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(760),
		Forecast:               helper.Float64ToSqlFloat64(7600),
		HandoverQty:            helper.Float64ToSqlFloat64(76),
		PickupQty:              helper.Float64ToSqlFloat64(76),
		ActualSaleValue:        helper.Float64ToSqlFloat64(760),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(7600),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(7600),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(7600),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	// Group by segment, group key = 1/1/3
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     19,
		SellerId:               1,
		GroupKey:               "1/1/3",
		GroupBy:                "segment",
		SiteId:                 0,
		NumberOfSkus:           1010,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(10100),
		Forecast:               helper.Float64ToSqlFloat64(101000),
		HandoverQty:            helper.Float64ToSqlFloat64(1010),
		PickupQty:              helper.Float64ToSqlFloat64(1010),
		ActualSaleValue:        helper.Float64ToSqlFloat64(10100),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(101000),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(101000),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(101000),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     20,
		SellerId:               1,
		GroupKey:               "1/1/3",
		GroupBy:                "segment",
		SiteId:                 1,
		NumberOfSkus:           805,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(8050),
		Forecast:               helper.Float64ToSqlFloat64(80500),
		HandoverQty:            helper.Float64ToSqlFloat64(805),
		PickupQty:              helper.Float64ToSqlFloat64(805),
		ActualSaleValue:        helper.Float64ToSqlFloat64(8050),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(80500),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(80500),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(80500),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:                     21,
		SellerId:               1,
		GroupKey:               "1/1/3",
		GroupBy:                "segment",
		SiteId:                 2,
		NumberOfSkus:           205,
		MonthOfYear:            monthOfYear,
		IsLatestGroup:          true,
		Budget:                 helper.Float64ToSqlFloat64(2050),
		Forecast:               helper.Float64ToSqlFloat64(20500),
		HandoverQty:            helper.Float64ToSqlFloat64(205),
		PickupQty:              helper.Float64ToSqlFloat64(205),
		ActualSaleValue:        helper.Float64ToSqlFloat64(2050),
		PreviousMonthSale:      helper.Float64ToSqlFloat64(20500),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(20500),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(20500),
		CreatedBy:              "creator",
		UpdatedBy:              "creator",
	}, true)

	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		SiteSkuDemandId: 2,
		Day:             1,
		Forecast:        1000,
		SellerId:        1,
		CreatedBy:       "system",
		UpdatedBy:       "system",
	}, true)
	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		SiteSkuDemandId: 4,
		Day:             2,
		Forecast:        2000,
		SellerId:        1,
		CreatedBy:       "system",
		UpdatedBy:       "system",
	}, true)

	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		SiteGroupDemandId: 6,
		Day:               3,
		Forecast:          3000,
		SellerId:          1,
		CreatedBy:         "system",
		UpdatedBy:         "system",
	}, true)
	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		SiteGroupDemandId: 13,
		Day:               4,
		Forecast:          4000,
		SellerId:          1,
		CreatedBy:         "system",
		UpdatedBy:         "system",
	}, true)
}

func (ts *jobCalculateDemandTestSuite) tearDown() {
	integrationtest.TruncateDatabases(ts.db,
		model.SiteSkuDemand{}.TableName(),
		model.SiteGroupDemand{}.TableName(),
		model.MonthlyCategory{}.TableName(),
		model.MonthlySkuSegmentPathMapping{}.TableName(),
		model.MonthlySkuCategoryMapping{}.TableName(),
		model.SiteSkuDailyForecast{}.TableName(),
		model.SiteGroupDailyForecast{}.TableName(),
	)
}

func (ts *jobCalculateDemandTestSuite) assert(job model.ScheduleJob, wantFile string) {
	_, err := ts.handler.Handle(ts.ctx, job)
	assert.Nil(ts.T(), err)

	var siteSkuDemands []*model.SiteSkuDemand
	err = ts.db.Table(model.SiteSkuDemand{}.TableName()).Find(&siteSkuDemands).Error
	assert.Nil(ts.T(), err)
	for _, record := range siteSkuDemands {
		record.Id = 0
		record.CreatedAt = ts.fixedTime
		record.UpdatedAt = ts.fixedTime
	}

	var siteGroupDemands []*model.SiteGroupDemand
	err = ts.db.Table(model.SiteGroupDemand{}.TableName()).Find(&siteGroupDemands).Error
	assert.Nil(ts.T(), err)
	for _, record := range siteGroupDemands {
		record.Id = 0
		record.CreatedAt = ts.fixedTime
		record.UpdatedAt = ts.fixedTime
	}

	var siteSkuDailyForecasts []*model.SiteSkuDailyForecast
	err = ts.db.Table(model.SiteSkuDailyForecast{}.TableName()).Find(&siteSkuDailyForecasts).Error
	assert.Nil(ts.T(), err)
	for _, record := range siteSkuDailyForecasts {
		record.Id = 0
		record.CreatedAt = ts.fixedTime
		record.UpdatedAt = ts.fixedTime
	}

	var siteGroupDailyForecasts []*model.SiteGroupDailyForecast
	err = ts.db.Table(model.SiteGroupDailyForecast{}.TableName()).Find(&siteGroupDailyForecasts).Error
	assert.Nil(ts.T(), err)
	for _, record := range siteGroupDailyForecasts {
		record.Id = 0
		record.CreatedAt = ts.fixedTime
		record.UpdatedAt = ts.fixedTime
	}

	goldie.New(ts.T()).AssertJson(ts.T(), wantFile, AssertObject{
		SiteSkuDemands:          siteSkuDemands,
		SiteGroupDemands:        siteGroupDemands,
		SiteSkuDailyForecasts:   siteSkuDailyForecasts,
		SiteGroupDailyForecasts: siteGroupDailyForecasts,
	})
}

func (ts *jobCalculateDemandTestSuite) Test200_UpdateSiteSkuDemand() {
	ts.tearDown()
	ts.setUp()
	defer ts.tearDown()

	payload := model.UpdateSkuDemandPayload{
		SellerId:          1,
		Sku:               "sku1",
		SiteId:            1,
		MonthOfYear:       "2023-11",
		Forecast:          1000000,
		NeedUpdateAllSite: true,
		UpdatedBy:         "user",
	}
	payloadStr, err := json.Marshal(payload)
	assert.Nil(ts.T(), err)
	ts.assert(model.ScheduleJob{
		Type:    model.ScheduleJobTypeUpdateSiteSkuDemand,
		Status:  int32(model.SchedulerJobStatusUnProcessed),
		Payload: helper.StringToSqlString(string(payloadStr)),
	}, "happy_case_update_site_sku_demand")
}

func (ts *jobCalculateDemandTestSuite) Test200_UpdateAllSiteSkuDemand() {
	ts.tearDown()
	ts.setUp()
	defer ts.tearDown()

	payload := model.UpdateSkuDemandPayload{
		SellerId:    1,
		Sku:         "sku2",
		SiteId:      0,
		MonthOfYear: "2023-11",
		Forecast:    10000000,
		UpdatedBy:   "user",
	}
	payloadStr, err := json.Marshal(payload)
	assert.Nil(ts.T(), err)
	ts.assert(model.ScheduleJob{
		Type:    model.ScheduleJobTypeUpdateAllSiteSkuDemand,
		Status:  int32(model.SchedulerJobStatusUnProcessed),
		Payload: helper.StringToSqlString(string(payloadStr)),
	}, "happy_case_update_all_site_sku_demand")
}

func (ts *jobCalculateDemandTestSuite) Test200_UpdateSiteGroupDemand() {
	ts.tearDown()
	ts.setUp()
	defer ts.tearDown()

	payload := model.UpdateGroupDemandPayload{
		SellerId:    1,
		GroupKey:    "1/2",
		GroupBy:     "category",
		SiteId:      2,
		MonthOfYear: "2023-11",
		Forecast:    2000000,
		UpdatedBy:   "user",
	}
	payloadStr, err := json.Marshal(payload)
	assert.Nil(ts.T(), err)
	ts.assert(model.ScheduleJob{
		Type:    model.ScheduleJobTypeUpdateSiteGroupDemand,
		Status:  int32(model.SchedulerJobStatusUnProcessed),
		Payload: helper.StringToSqlString(string(payloadStr)),
	}, "happy_case_update_site_group_demand")
}

func (ts *jobCalculateDemandTestSuite) Test200_UpdateAllSiteGroupDemand() {
	ts.tearDown()
	ts.setUp()
	defer ts.tearDown()

	payload := model.UpdateGroupDemandPayload{
		SellerId:    1,
		GroupKey:    "1/1",
		GroupBy:     "segment",
		SiteId:      0,
		MonthOfYear: "2023-11",
		Forecast:    5000000,
		UpdatedBy:   "user",
	}
	payloadStr, err := json.Marshal(payload)
	assert.Nil(ts.T(), err)
	ts.assert(model.ScheduleJob{
		Type:    model.ScheduleJobTypeUpdateAllSiteGroupDemand,
		Status:  int32(model.SchedulerJobStatusUnProcessed),
		Payload: helper.StringToSqlString(string(payloadStr)),
	}, "happy_case_update_all_site_group_demand")
}

func (ts *jobCalculateDemandTestSuite) Test200_UpdateSiteSkuDemandSkipResetDailyForecast() {
	ts.tearDown()
	ts.setUp()
	defer ts.tearDown()

	payload := model.UpdateSkuDemandPayload{
		SellerId:                         1,
		Sku:                              "sku1",
		SiteId:                           1,
		MonthOfYear:                      "2023-11",
		Forecast:                         1000000,
		DemandIdIgnoreResetDailyForecast: 2,
		NeedUpdateAllSite:                true,
		UpdatedBy:                        "user",
	}
	payloadStr, err := json.Marshal(payload)
	assert.Nil(ts.T(), err)
	ts.assert(model.ScheduleJob{
		Type:    model.ScheduleJobTypeUpdateSiteSkuDemand,
		Status:  int32(model.SchedulerJobStatusUnProcessed),
		Payload: helper.StringToSqlString(string(payloadStr)),
	}, "happy_case_update_site_sku_demand_skip_reset_daily_forecast")
}

func (ts *jobCalculateDemandTestSuite) Test200_UpdateSiteGroupDemandSkipResetDailyForecast() {
	ts.tearDown()
	ts.setUp()
	defer ts.tearDown()

	payload := model.UpdateGroupDemandPayload{
		SellerId:                         1,
		GroupKey:                         "1/2",
		GroupBy:                          "category",
		SiteId:                           2,
		MonthOfYear:                      "2023-11",
		Forecast:                         2000000,
		DemandIdIgnoreResetDailyForecast: 6,
		UpdatedBy:                        "user",
	}
	payloadStr, err := json.Marshal(payload)
	assert.Nil(ts.T(), err)
	ts.assert(model.ScheduleJob{
		Type:    model.ScheduleJobTypeUpdateSiteGroupDemand,
		Status:  int32(model.SchedulerJobStatusUnProcessed),
		Payload: helper.StringToSqlString(string(payloadStr)),
	}, "happy_case_update_site_group_demand_skip_reset_daily_forecast")
}
