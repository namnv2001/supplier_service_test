package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/api"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/errorz"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/integrationtest"
)

type getDemandPlanningSuite struct {
	tests.TestSuite
	faker *faker.Faker
	*gorm.DB
}

func TestGetDemandPlanning(t *testing.T) {
	ts := &getDemandPlanningSuite{}
	db, svsLog := ts.InitIntegrationTestMain(t, gormDb, logService)
	defer func(db *gorm.DB) {
		ts.tearDown(db)
	}(db)

	svc := &service.Service{
		SiteSkuDemandRepository:                repository.NewSiteSkuDemandRepository(db),
		SiteGroupDemandRepository:              repository.NewSiteGroupDemandRepository(db),
		MonthlyCategoryRepository:              repository.NewMonthlyCategoryRepository(db),
		MonthlySkuCategoryMappingRepository:    repository.NewMonthlySkuCategoryMappingRepository(db),
		MonthlySkuSegmentPathMappingRepository: repository.NewMonthlySkuSegmentPathMappingRepository(db),
		MonthlyVariantAttributeRepository:      repository.NewMonthlyVariantAttributeRepository(db),
		Log:                                    svsLog,
	}

	conn, server := ts.InitServerAndClient(t, svc)

	ts.faker = &faker.Faker{DB: db}
	ts.DB = db

	suite.Run(t, ts)
	defer func() {
		err := conn.Close()
		if err != nil {
			svsLog.Error(err, "could not close connection")
		}
		server.Stop()
	}()
}

func (ts *getDemandPlanningSuite) tearDown(db *gorm.DB) {
	integrationtest.TruncateDatabases(db,
		model.SiteSkuDemand{}.TableName(),
		model.SiteGroupDemand{}.TableName(),
		model.MonthlyCategory{}.TableName(),
		model.MonthlySkuCategoryMapping{}.TableName(),
		model.MonthlySkuSegmentPathMapping{}.TableName(),
		model.MonthlyVariantAttribute{}.TableName(),
	)
}

func (ts *getDemandPlanningSuite) assertSuccess(req *api.GetDemandPlanningRequest, want *api.GetDemandPlanningResponse) {
	res, err := ts.DemandPlanningClient.GetDemandPlanning(ts.Context, req)
	assert.Nil(ts.T(), err)
	assert.NotNil(ts.T(), res)
	assert.Equal(ts.T(), want.Code, res.Code)
	assert.Equal(ts.T(), want.Message, res.Message)
	assert.Equal(ts.T(), len(want.Data.DemandPlannings), len(res.Data.DemandPlannings))
	assert.Equal(ts.T(), want.Data.DemandPlannings[0].Name, res.Data.DemandPlannings[0].Name)
}

// case with isLatestGroupTrue
// get from site_sku_demand table
// sku name get by join with monthly_sku_category_mapping
// case have groupBy and groupKey filter
// get list skus from monthly_sku_category_mapping
// with latest_child_category_id = 2, got sku = sku2
// then find in site_sku_demand with this filter.
func (ts *getDemandPlanningSuite) TestSuccess_WithIsLatestGroup_True_And_GroupBy_Category() {

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		SellerId:               1,
		Sku:                    "sku1",
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
		AvgSellPrice:           helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		SellerId:               1,
		Sku:                    "sku2",
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
		AvgSellPrice:           helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku1",
		SellerSku:             "ssku1",
		Name:                  "SkuName1",
		MonthOfYear:           "2022-08",
		LatestChildCategoryId: 1,
	}, true)

	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku2",
		SellerSku:             "ssku2",
		Name:                  "SkuName2",
		MonthOfYear:           "2022-08",
		LatestChildCategoryId: 2,
	}, true)

	requestInDate := time.Date(2022, 8, 15, 1, 0, 0, 0, time.UTC)

	req := &api.GetDemandPlanningRequest{
		SellerId:      1,
		SiteId:        1,
		Page:          1,
		PageSize:      10,
		RequestedDate: int32(requestInDate.Unix()),
		IsLatestGroup: true,
		GroupBy:       helper.StringToProtoString("category"),
		GroupKey:      helper.StringToProtoString("1/2"),
	}

	want := &api.GetDemandPlanningResponse{
		Code:    code.Code(codes.OK),
		Message: errorz.MessageSuccess,
		Data: &api.GetDemandPlanningResponse_Data{
			Pagination: &api.GetDemandPlanningResponse_Pagination{
				Page:     1,
				PageSize: 10,
				Total:    1,
			},
			DemandPlannings: []*api.GetDemandPlanningResponse_DemandPlannings{
				{
					GroupDemandId:          2,
					Name:                   "[ssku2] SkuName2",
					IsLatestGroup:          true,
					IsSkuDemand:            true,
					PreviousMonthSale:      helper.Float64ToProtoDouble(1),
					Budget:                 helper.Float64ToProtoDouble(1),
					Forecast:               helper.Float64ToProtoDouble(1),
					ActualSale:             helper.Float64ToProtoDouble(1),
					CurrentInventoryValue:  helper.Float64ToProtoDouble(1),
					ForecastInventoryValue: helper.Float64ToProtoDouble(1),
					AvgSellPrice:           helper.Float64ToProtoDouble(1),
				},
			},
		},
	}

	ts.assertSuccess(req, want)
	ts.tearDown(ts.DB)
}

// case with isLatestGroupTrue
// get from site_sku_demand table
// sku name get by join with monthly_sku_category_mapping
// case have groupBy and groupKey filter
// get list skus from monthly_sku_segment_path_mapping
// with groupKey = 1/2/3, segment = 2/3, got sku = sku3
// then find in site_sku_demand with this filter.
func (ts *getDemandPlanningSuite) TestSuccess_WithIsLatestGroup_True_And_GroupBy_Segment() {

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		SellerId:               1,
		Sku:                    "sku1",
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
		AvgSellPrice:           helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		SellerId:               1,
		Sku:                    "sku2",
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
		AvgSellPrice:           helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		SellerId:               1,
		Sku:                    "sku3",
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
		AvgSellPrice:           helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku2",
		SellerSku:             "ssku2",
		Name:                  "SkuName2",
		MonthOfYear:           "2022-08",
		LatestChildCategoryId: 2,
	}, true)

	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku3",
		SellerSku:             "ssku3",
		Name:                  "SkuName3",
		MonthOfYear:           "2022-08",
		LatestChildCategoryId: 2,
	}, true)

	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:    1,
		Sku:         "sku1",
		MonthOfYear: "2022-08",
		SegmentPath: "1/2",
	}, true)

	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:    1,
		Sku:         "sku2",
		MonthOfYear: "2022-08",
		SegmentPath: "2/3/4",
	}, true)

	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:    1,
		Sku:         "sku3",
		MonthOfYear: "2022-08",
		SegmentPath: "2/3",
	}, true)

	requestInDate := time.Date(2022, 8, 15, 1, 0, 0, 0, time.UTC)

	req := &api.GetDemandPlanningRequest{
		SellerId:      1,
		SiteId:        1,
		Page:          1,
		PageSize:      10,
		RequestedDate: int32(requestInDate.Unix()),
		IsLatestGroup: true,
		GroupBy:       helper.StringToProtoString("segment"),
		GroupKey:      helper.StringToProtoString("1/2/3"),
	}

	want := &api.GetDemandPlanningResponse{
		Code:    code.Code(codes.OK),
		Message: errorz.MessageSuccess,
		Data: &api.GetDemandPlanningResponse_Data{
			Pagination: &api.GetDemandPlanningResponse_Pagination{
				Page:     1,
				PageSize: 10,
				Total:    1,
			},
			DemandPlannings: []*api.GetDemandPlanningResponse_DemandPlannings{
				{
					GroupDemandId:          3,
					Name:                   "[ssku3] SkuName3",
					IsSkuDemand:            true,
					PreviousMonthSale:      helper.Float64ToProtoDouble(1),
					Budget:                 helper.Float64ToProtoDouble(1),
					Forecast:               helper.Float64ToProtoDouble(1),
					ActualSale:             helper.Float64ToProtoDouble(1),
					CurrentInventoryValue:  helper.Float64ToProtoDouble(1),
					ForecastInventoryValue: helper.Float64ToProtoDouble(1),
					AvgSellPrice:           helper.Float64ToProtoDouble(1),
				},
			},
		},
	}

	ts.assertSuccess(req, want)
	ts.tearDown(ts.DB)
}

func (ts *getDemandPlanningSuite) TestSuccess_WithIsLatestGroupFalse_WithFilterGroupKey() {

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          true,
		GroupKey:               "1/2/3",
		GroupBy:                "segment",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		SellerId:         1,
		AttributeValueId: 3,
		DisplayValue:     "Option3",
		MonthOfYear:      "2022-08",
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          false,
		GroupKey:               "1/2/3/4",
		GroupBy:                "segment",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          false,
		GroupKey:               "4/2/3",
		GroupBy:                "segment",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	requestInDate := time.Date(2022, 8, 15, 1, 0, 0, 0, time.UTC)

	req := &api.GetDemandPlanningRequest{
		SellerId:      1,
		SiteId:        1,
		Page:          1,
		PageSize:      10,
		RequestedDate: int32(requestInDate.Unix()),
		IsLatestGroup: false,
		GroupKey:      helper.StringToProtoString("1/2"),
	}

	want := &api.GetDemandPlanningResponse{
		Code:    code.Code(codes.OK),
		Message: errorz.MessageSuccess,
		Data: &api.GetDemandPlanningResponse_Data{
			Pagination: &api.GetDemandPlanningResponse_Pagination{
				Page:     1,
				PageSize: 10,
				Total:    1,
			},
			DemandPlannings: []*api.GetDemandPlanningResponse_DemandPlannings{
				{
					GroupDemandId:          1,
					IsLatestGroup:          true,
					IsSkuDemand:            false,
					GroupKey:               helper.StringToProtoString("1/2/3"),
					Name:                   "Option3",
					PreviousMonthSale:      helper.Float64ToProtoDouble(1),
					Budget:                 helper.Float64ToProtoDouble(1),
					Forecast:               helper.Float64ToProtoDouble(1),
					ActualSale:             helper.Float64ToProtoDouble(1),
					CurrentInventoryValue:  helper.Float64ToProtoDouble(1),
					ForecastInventoryValue: helper.Float64ToProtoDouble(1),
					AvgSellPrice:           helper.Float64ToProtoDouble(1),
				},
			},
		},
	}

	ts.assertSuccess(req, want)
	ts.tearDown(ts.DB)
}

func (ts *getDemandPlanningSuite) TestSuccess_WithIsLatestGroupFalse_WithFilterCategoryId() {

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          true,
		GroupKey:               "1/2",
		GroupBy:                "category",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  2,
		Code:        "CATE2",
		Name:        "Category2",
		MonthOfYear: "2022-08",
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  1,
		Code:        "CATE1",
		Name:        "Category1",
		MonthOfYear: "2022-08",
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          false,
		GroupKey:               "1/2/3/4",
		GroupBy:                "segment",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          false,
		GroupKey:               "4/2/3",
		GroupBy:                "segment",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          false,
		GroupKey:               "1",
		GroupBy:                "segment",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:               1,
		MonthOfYear:            "2022-08",
		SiteId:                 1,
		IsLatestGroup:          false,
		GroupKey:               "2/1",
		GroupBy:                "segment",
		PreviousMonthSale:      helper.Float64ToSqlFloat64(1),
		Budget:                 helper.Float64ToSqlFloat64(1),
		Forecast:               helper.Float64ToSqlFloat64(1),
		ActualSaleValue:        helper.Float64ToSqlFloat64(1),
		CurrentInventoryValue:  helper.Float64ToSqlFloat64(1),
		ForecastInventoryValue: helper.Float64ToSqlFloat64(1),
	}, true)

	requestInDate := time.Date(2022, 8, 15, 1, 0, 0, 0, time.UTC)

	req := &api.GetDemandPlanningRequest{
		SellerId:      1,
		SiteId:        1,
		Page:          1,
		PageSize:      10,
		RequestedDate: int32(requestInDate.Unix()),
		GroupBy:       helper.StringToProtoString("segment"),
		IsLatestGroup: false,
		CategoryId:    helper.Int32ToProtoInt32(1),
	}

	want := &api.GetDemandPlanningResponse{
		Code:    code.Code(codes.OK),
		Message: errorz.MessageSuccess,
		Data: &api.GetDemandPlanningResponse_Data{
			Pagination: &api.GetDemandPlanningResponse_Pagination{
				Page:     1,
				PageSize: 10,
				Total:    1,
			},
			DemandPlannings: []*api.GetDemandPlanningResponse_DemandPlannings{
				{
					GroupDemandId:          1,
					IsLatestGroup:          false,
					IsSkuDemand:            false,
					Name:                   "[CATE1] Category1",
					GroupKey:               helper.StringToProtoString("1"),
					PreviousMonthSale:      helper.Float64ToProtoDouble(1),
					Budget:                 helper.Float64ToProtoDouble(1),
					Forecast:               helper.Float64ToProtoDouble(1),
					ActualSale:             helper.Float64ToProtoDouble(1),
					CurrentInventoryValue:  helper.Float64ToProtoDouble(1),
					ForecastInventoryValue: helper.Float64ToProtoDouble(1),
					AvgSellPrice:           helper.Float64ToProtoDouble(1),
				},
			},
		},
	}

	ts.assertSuccess(req, want)
	ts.tearDown(ts.DB)
}
