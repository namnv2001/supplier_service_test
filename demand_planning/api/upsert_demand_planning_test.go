package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/api"
	mockFlagsup "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/flagsup"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/provider"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type upsertDemandPlanningSuite struct {
	tests.TestSuite
	faker *faker.Faker
	*gorm.DB
	fixedTime     time.Time
	nextFixedTime time.Time
	prevFixedTime time.Time
	mapFlag       map[string]bool
}

type upsertDemandPlanning struct {
	Resp             *api.UpsertDemandPlanningResponse
	OriginalBudget   []*model.OriginalBudget
	SiteGroupDemands []*model.SiteGroupDemand
	ScheduleJobs     []*model.ScheduleJob
}

func TestUpsertDemandPlanning_CaseImport(t *testing.T) {
	ts := &upsertDemandPlanningSuite{}
	db, svsLog := ts.InitIntegrationTestMain(t, gormDb, logService)
	ts.tearDown(db)
	monkey.UnpatchAll()

	ts.mapFlag = map[string]bool{}

	flagsupClient := &mockFlagsup.ClientAdapter{}
	flagsupClient.Mock.On("IsEnabled", mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, flagKey string, fallback bool) bool {
			return ts.mapFlag[flagKey]
		})

	scheduleJobProvider := provider.NewScheduleJobProvider(db, flagsupClient)
	svc := &service.Service{
		SiteGroupDemandRepository:         repository.NewSiteGroupDemandRepository(db),
		MonthlyCategoryRepository:         repository.NewMonthlyCategoryRepository(db),
		MonthlyVariantAttributeRepository: repository.NewMonthlyVariantAttributeRepository(db),
		OriginalBudgetRepository:          repository.NewOriginalBudgetRepository(db),
		ScheduleJobProvider:               scheduleJobProvider,
		Log:                               svsLog,
		DB:                                db,
	}
	conn, server := ts.InitServerAndClient(t, svc)

	ts.faker = &faker.Faker{DB: db}
	ts.DB = db
	ts.fixedTime = time.Date(2022, time.February, 9, 1, 0, 0, 0, time.UTC)
	ts.nextFixedTime = time.Date(2022, time.March, 10, 1, 0, 0, 0, time.UTC)
	ts.prevFixedTime = time.Date(2022, time.January, 8, 1, 0, 0, 0, time.UTC)

	monkey.Patch(time.Now, func() time.Time { return ts.fixedTime })

	defer func() {
		err := conn.Close()
		if err != nil {
			svsLog.Error(err, "could not close connection")
		}
		server.Stop()
		ts.tearDown(db)
		monkey.UnpatchAll()
	}()

	suite.Run(t, ts)
}

func (ts *upsertDemandPlanningSuite) tearDown(db *gorm.DB) {
	integrationtest.TruncateDatabases(db,
		model.SiteGroupDemand{}.TableName(),
		model.OriginalBudget{}.TableName(),
		model.MonthlyCategory{}.TableName(),
		model.MonthlyVariantAttribute{}.TableName(),
		model.ScheduleJob{}.TableName(),
	)
}

func (ts *upsertDemandPlanningSuite) assert(req *api.UpsertDemandPlanningRequest, wantFile string) {
	defer func() {
		ts.tearDown(ts.DB)
	}()
	res, err := ts.DemandPlanningClient.UpsertDemandPlanning(ts.Context, req)
	g := goldie.New(ts.T())
	prefix := "upsert_demand_planning"
	if err != nil {
		assert.Nil(ts.T(), res)
		assert.Error(ts.T(), err)
		assert.Equal(ts.T(), strings.HasPrefix(wantFile, "error_"), true)
		g.AssertJson(ts.T(), fmt.Sprintf("%s/%s", prefix, wantFile), err.Error())
		return
	}
	var wantData upsertDemandPlanning
	res.TraceId = ""
	wantData.Resp = res

	var originalBudget []*model.OriginalBudget
	err = ts.DB.Table(model.OriginalBudget{}.TableName()).Order("id").Find(&originalBudget).Error
	if err != nil {
		ts.T().Fatal("error find OriginalBudget")
	}
	for _, budgetInfo := range originalBudget {
		budgetInfo.CreatedAt = ts.fixedTime
		budgetInfo.UpdatedAt = ts.fixedTime
	}
	wantData.OriginalBudget = originalBudget

	var siteGroupDemands []*model.SiteGroupDemand
	err = ts.DB.Table(model.SiteGroupDemand{}.TableName()).Order("id").Find(&siteGroupDemands).Error
	if err != nil {
		ts.T().Fatal("error find SiteSkuDemand")
	}
	for _, siteGroupDemand := range siteGroupDemands {
		siteGroupDemand.CreatedAt = ts.fixedTime
		siteGroupDemand.UpdatedAt = ts.fixedTime
	}
	wantData.SiteGroupDemands = siteGroupDemands

	var scheduleJobs []*model.ScheduleJob
	err = ts.DB.Table(model.ScheduleJob{}.TableName()).Order("id").Find(&scheduleJobs).Error
	if err != nil {
		ts.T().Fatal("error find ScheduleJob")
	}
	for _, scheduleJob := range scheduleJobs {
		scheduleJob.CreatedAt = ts.fixedTime
		scheduleJob.UpdatedAt = ts.fixedTime
		scheduleJob.ExecutedAt = ts.fixedTime
	}
	wantData.ScheduleJobs = scheduleJobs

	g.AssertJson(ts.T(), fmt.Sprintf("%s/%s", prefix, wantFile), wantData)
}

func (ts *upsertDemandPlanningSuite) Test200_HappyCase_ImportBudget() {
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  1,
		Code:        "CATE1",
		Name:        "cate 1",
		MonthOfYear: helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  2,
		Code:        "CATE2",
		Name:        "cate 2",
		ParentId:    helper.Int32ToSqlInt32(1),
		MonthOfYear: helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)

	req := &api.UpsertDemandPlanningRequest{
		SellerId:       1,
		IsSkuDemand:    false,
		UpdatedByEmail: "email@gmail.com",
		CategoryId:     helper.Int32ToProtoInt32(1),
		SiteId:         helper.Int32ToProtoInt32(1),
		BudgetByDates: []*api.UpsertDemandPlanningRequest_BudgetByDate{
			{
				RequestedDate: int32(ts.fixedTime.Unix()),
				Budget:        100000,
			},
			{
				RequestedDate: int32(ts.nextFixedTime.Unix()),
				Budget:        2000000,
			},
		},
	}
	ts.assert(req, "happy_case_import_budget")
}

func (ts *upsertDemandPlanningSuite) Test400_ErrorCase_ImportBudget_CategoryIsNotMaster() {
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  1,
		Code:        "CATE1",
		Name:        "cate 1",
		MonthOfYear: helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  2,
		Code:        "CATE2",
		Name:        "cate 2",
		ParentId:    helper.Int32ToSqlInt32(1),
		MonthOfYear: helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)

	req := &api.UpsertDemandPlanningRequest{
		SellerId:       1,
		IsSkuDemand:    false,
		UpdatedByEmail: "email@gmail.com",
		CategoryId:     helper.Int32ToProtoInt32(2),
		SiteId:         helper.Int32ToProtoInt32(1),
		BudgetByDates: []*api.UpsertDemandPlanningRequest_BudgetByDate{
			{
				RequestedDate: int32(ts.fixedTime.Unix()),
				Budget:        100000,
			},
			{
				RequestedDate: int32(ts.nextFixedTime.Unix()),
				Budget:        2000000,
			},
		},
	}
	ts.assert(req, "error_case_import_budget_category_is_not_master")
}

func (ts *upsertDemandPlanningSuite) Test200_HappyCase_ImportForecast_CaseCategory() {
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  1,
		Code:        "CATE1",
		Name:        "Category 1",
		MonthOfYear: helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  2,
		Code:        "CATE2",
		Name:        "Category 2",
		ParentId:    helper.Int32ToSqlInt32(1),
		MonthOfYear: helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  3,
		Code:        "CATE3",
		Name:        "Category 3",
		ParentId:    helper.Int32ToSqlInt32(2),
		MonthOfYear: helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:      1,
		GroupKey:      "1/2/3",
		GroupBy:       "segment",
		SiteId:        1,
		NumberOfSkus:  20,
		MonthOfYear:   helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
		IsLatestGroup: false,
		Budget:        helper.Float64ToSqlFloat64(1200),
		Forecast:      helper.Float64ToSqlFloat64(15000),
		CreatedBy:     "abc@gmail.com",
		UpdatedBy:     "abc@gmail.com",
	}, true)

	req := &api.UpsertDemandPlanningRequest{
		SellerId:           1,
		IsSkuDemand:        false,
		UpdatedByEmail:     "email@gmail.com",
		SiteId:             helper.Int32ToProtoInt32(1),
		CategoryId:         helper.Int32ToProtoInt32(1),
		ListCategoryCode:   []string{"CATE2", "CATE3"},
		Forecast:           2000,
		ImportForecastDate: helper.Int32ToProtoInt32(int32(ts.fixedTime.Unix())),
	}
	ts.assert(req, "happy_case_import_forecast_case_category")
}

func (ts *upsertDemandPlanningSuite) Test200_HappyCase_ImportForecast_CaseSegment() {
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 1,
		SellerId:         1,
		Sku:              "sku1",
		AttributeId:      1,
		AttributeCode:    "ATT1",
		Value:            "ATT value 1",
		DisplayValue:     "ATT value 1",
		MonthOfYear:      helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 2,
		SellerId:         1,
		Sku:              "sku2",
		AttributeId:      1,
		AttributeCode:    "ATT1",
		Value:            "ATT value 2",
		DisplayValue:     "ATT value 2",
		MonthOfYear:      helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 3,
		SellerId:         1,
		Sku:              "sku1",
		AttributeId:      3,
		AttributeCode:    "ATT3",
		Value:            "ATT value 3",
		DisplayValue:     "ATT value 3",
		MonthOfYear:      helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:      1,
		GroupKey:      "1/1/2/3",
		GroupBy:       "segment",
		SiteId:        1,
		NumberOfSkus:  20,
		MonthOfYear:   helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
		IsLatestGroup: false,
		Budget:        helper.Float64ToSqlFloat64(1200),
		Forecast:      helper.Float64ToSqlFloat64(15000),
		CreatedBy:     "abc@gmail.com",
		UpdatedBy:     "abc@gmail.com",
	}, true)

	req := &api.UpsertDemandPlanningRequest{
		SellerId:           1,
		IsSkuDemand:        false,
		UpdatedByEmail:     "email@gmail.com",
		SiteId:             helper.Int32ToProtoInt32(1),
		CategoryId:         helper.Int32ToProtoInt32(1),
		ListAttributeValue: []string{"ATT value 1", "ATT value 2", "ATT value 3"},
		Forecast:           20,
		ImportForecastDate: helper.Int32ToProtoInt32(int32(ts.fixedTime.Unix())),
	}
	ts.assert(req, "happy_case_import_forecast_case_segment")
}

func (ts *upsertDemandPlanningSuite) Test200_HappyCase_ImportForecast_InsertInFuture() {
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 1,
		SellerId:         1,
		Sku:              "sku1",
		AttributeId:      1,
		AttributeCode:    "ATT1",
		Value:            "ATT value 1",
		DisplayValue:     "ATT value 1",
		MonthOfYear:      helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 2,
		SellerId:         1,
		Sku:              "sku2",
		AttributeId:      1,
		AttributeCode:    "ATT1",
		Value:            "ATT value 2",
		DisplayValue:     "ATT value 2",
		MonthOfYear:      helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 3,
		SellerId:         1,
		Sku:              "sku1",
		AttributeId:      3,
		AttributeCode:    "ATT3",
		Value:            "ATT value 3",
		DisplayValue:     "ATT value 3",
		MonthOfYear:      helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		SellerId:      1,
		GroupKey:      "1/1/2/3",
		GroupBy:       "segment",
		SiteId:        1,
		NumberOfSkus:  20,
		MonthOfYear:   helper.ConvertInt32DateToMonthOfYear(int32(ts.fixedTime.Unix())),
		IsLatestGroup: false,
		Budget:        helper.Float64ToSqlFloat64(1200),
		Forecast:      helper.Float64ToSqlFloat64(15000),
		CreatedBy:     "abc@gmail.com",
		UpdatedBy:     "abc@gmail.com",
	}, true)

	req := &api.UpsertDemandPlanningRequest{
		SellerId:           1,
		IsSkuDemand:        false,
		UpdatedByEmail:     "email@gmail.com",
		SiteId:             helper.Int32ToProtoInt32(1),
		CategoryId:         helper.Int32ToProtoInt32(1),
		ListAttributeValue: []string{"ATT value 1", "ATT value 2", "ATT value 3"},
		Forecast:           200,
		ImportForecastDate: helper.Int32ToProtoInt32(int32(ts.nextFixedTime.Unix())),
	}
	ts.assert(req, "happy_case_import_forecast_insert_in_future")
}

func (ts *upsertDemandPlanningSuite) Test400_ErrorCase_ImportForecast_OnlySegmentOrCategory() {
	req := &api.UpsertDemandPlanningRequest{
		SellerId:           1,
		IsSkuDemand:        false,
		UpdatedByEmail:     "email@gmail.com",
		SiteId:             helper.Int32ToProtoInt32(1),
		CategoryId:         helper.Int32ToProtoInt32(1),
		ListAttributeValue: []string{"ATT value 1", "ATT value 2", "ATT value 3"},
		ListCategoryCode:   []string{"CATE 1", "CATE 2", "CATE 3"},
		Forecast:           20,
		ImportForecastDate: helper.Int32ToProtoInt32(int32(ts.fixedTime.Unix())),
	}
	ts.assert(req, "error_case_import_forecast_only_segment_or_category")
}

func (ts *upsertDemandPlanningSuite) Test400_ErrorCase_ImportForecast_RequiredSegmentOrCategory() {
	req := &api.UpsertDemandPlanningRequest{
		SellerId:           1,
		IsSkuDemand:        false,
		UpdatedByEmail:     "email@gmail.com",
		SiteId:             helper.Int32ToProtoInt32(1),
		CategoryId:         helper.Int32ToProtoInt32(1),
		Forecast:           20,
		ImportForecastDate: helper.Int32ToProtoInt32(int32(ts.fixedTime.Unix())),
	}
	ts.assert(req, "error_case_import_forecast_required_segment_or_category")
}

func (ts *upsertDemandPlanningSuite) Test400_ErrorCase_ImportForecast_UpdateInPast() {
	req := &api.UpsertDemandPlanningRequest{
		SellerId:           1,
		IsSkuDemand:        false,
		UpdatedByEmail:     "email@gmail.com",
		SiteId:             helper.Int32ToProtoInt32(1),
		CategoryId:         helper.Int32ToProtoInt32(1),
		ListCategoryCode:   []string{"CATE 1", "CATE 2", "CATE 3"},
		Forecast:           20,
		ImportForecastDate: helper.Int32ToProtoInt32(int32(ts.prevFixedTime.Unix())),
	}
	ts.assert(req, "error_case_import_forecast_update_in_past")
}
