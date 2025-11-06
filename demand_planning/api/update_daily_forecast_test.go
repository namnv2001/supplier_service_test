package api

import (
	"context"
	"database/sql"
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
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type updateDailyForecastSuite struct {
	tests.TestSuite
	faker *faker.Faker
	*gorm.DB
	fixedTime time.Time
	mapFlag   map[string]bool
}

type updateDailForecastObj struct {
	Resp             *api.UpdateDailyForecastResponse
	SiteSkuDemands   []*model.SiteSkuDemand
	SiteGroupDemands []*model.SiteGroupDemand
	ScheduleJobs     []*model.ScheduleJob
}

func TestUpdateDailyForecast(t *testing.T) {
	ts := &updateDailyForecastSuite{}
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
		SiteSkuDailyForecastRepository:   repository.NewSiteSkuDailyForecastRepository(db),
		SiteGroupDailyForecastRepository: repository.NewSiteGroupDailyForecastRepository(db),
		SiteSkuDemandRepository:          repository.NewSiteSkuDemandRepository(db),
		SiteGroupDemandRepository:        repository.NewSiteGroupDemandRepository(db),
		ScheduleJobProvider:              scheduleJobProvider,
		Log:                              svsLog,
		DB:                               db,
	}
	conn, server := ts.InitServerAndClient(t, svc)

	ts.faker = &faker.Faker{DB: db}
	ts.DB = db
	ts.fixedTime = time.Date(2022, time.January, 9, 1, 0, 0, 0, time.UTC)

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

func (ts *updateDailyForecastSuite) tearDown(db *gorm.DB) {
	integrationtest.TruncateDatabases(db,
		model.SiteSkuDailyForecast{}.TableName(),
		model.SiteSkuDemand{}.TableName(),
		model.SiteGroupDailyForecast{}.TableName(),
		model.SiteGroupDemand{}.TableName(),
		model.ScheduleJob{}.TableName(),
	)
}

func (ts *updateDailyForecastSuite) assert(req *api.UpdateDailyForecastRequest, wantFile string) {
	defer func() {
		ts.tearDown(ts.DB)
	}()
	res, err := ts.DemandPlanningClient.UpdateDailyForecast(ts.Context, req)
	g := goldie.New(ts.T())
	prefix := "update_daily_forecast"
	if err != nil {
		assert.Nil(ts.T(), res)
		assert.Error(ts.T(), err)
		assert.Equal(ts.T(), strings.HasPrefix(wantFile, "error_"), true)
		g.AssertJson(ts.T(), fmt.Sprintf("%s/%s", prefix, wantFile), err.Error())
		return
	}
	var wantData updateDailForecastObj
	res.TraceId = ""
	wantData.Resp = res

	var siteSkuDemands []*model.SiteSkuDemand
	err = ts.DB.Table(model.SiteSkuDemand{}.TableName()).Order("id").Find(&siteSkuDemands).Error
	if err != nil {
		ts.T().Fatal("error find SiteSkuDemand")
	}
	for _, siteSkuDemand := range siteSkuDemands {
		siteSkuDemand.CreatedAt = ts.fixedTime
		siteSkuDemand.UpdatedAt = ts.fixedTime
	}
	wantData.SiteSkuDemands = siteSkuDemands

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

func (ts *updateDailyForecastSuite) Test200_happyCase_siteSkuDemand() {
	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		Id:              1,
		SellerId:        1,
		SiteSkuDemandId: 1,
		Day:             9,
		Forecast:        float64(10),
	}, true)
	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		Id:              2,
		SellerId:        1,
		SiteSkuDemandId: 1,
		Day:             20,
		Forecast:        float64(10),
	}, true)

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:          1,
		SellerId:    1,
		MonthOfYear: "2022-01",
		SiteId:      1,
		Forecast:    sql.NullFloat64{Valid: true, Float64: 300},
	}, true)

	req := &api.UpdateDailyForecastRequest{
		SellerId:        1,
		IsSkuDemand:     true,
		DailyForecastId: 1,
		Forecast:        20,
		UpdatedByEmail:  "email@gmail.com",
	}
	ts.assert(req, "success_happy_case_site_sku_demand")
}

func (ts *updateDailyForecastSuite) Test400_siteSkuDemand_update_time_in_past() {
	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		Id:              1,
		SellerId:        1,
		SiteSkuDemandId: 1,
		Day:             8,
		Forecast:        float64(10),
	}, true)
	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		Id:              2,
		SellerId:        1,
		SiteSkuDemandId: 1,
		Day:             20,
		Forecast:        float64(10),
	}, true)

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:          1,
		SellerId:    1,
		MonthOfYear: "2022-01",
		Forecast:    sql.NullFloat64{Valid: true, Float64: 300},
	}, true)

	req := &api.UpdateDailyForecastRequest{
		SellerId:        1,
		IsSkuDemand:     true,
		DailyForecastId: 1,
		Forecast:        20,
		UpdatedByEmail:  "email@gmail.com",
	}
	ts.assert(req, "error_update_time_in_past_site_sku_demand")
}

func (ts *updateDailyForecastSuite) Test200_happyCase_siteGroupDemand() {
	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		Id:                1,
		SellerId:          1,
		SiteGroupDemandId: 1,
		Day:               9,
		Forecast:          float64(10),
	}, true)
	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		Id:                2,
		SellerId:          1,
		SiteGroupDemandId: 1,
		Day:               20,
		Forecast:          float64(10),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          1,
		SellerId:    1,
		GroupBy:     model.SiteGroupDemandGroupBySegment,
		GroupKey:    "1",
		SiteId:      0,
		MonthOfYear: "2022-01",
		Forecast:    sql.NullFloat64{Valid: true, Float64: 300},
	}, true)

	req := &api.UpdateDailyForecastRequest{
		SellerId:        1,
		IsSkuDemand:     false,
		DailyForecastId: 1,
		Forecast:        20,
		UpdatedByEmail:  "email@gmail.com",
	}
	ts.assert(req, "success_happy_case_site_group_demand")
}

func (ts *updateDailyForecastSuite) Test400_siteGroupDemand_update_time_in_past() {
	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		Id:                1,
		SellerId:          1,
		SiteGroupDemandId: 1,
		Day:               8,
		Forecast:          float64(10),
	}, true)
	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		Id:                2,
		SellerId:          1,
		SiteGroupDemandId: 1,
		Day:               20,
		Forecast:          float64(10),
	}, true)

	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          1,
		SellerId:    1,
		GroupBy:     model.SiteGroupDemandGroupBySegment,
		MonthOfYear: "2022-01",
		Forecast:    sql.NullFloat64{Valid: true, Float64: 300},
	}, true)

	req := &api.UpdateDailyForecastRequest{
		SellerId:        1,
		IsSkuDemand:     false,
		DailyForecastId: 1,
		Forecast:        20,
		UpdatedByEmail:  "email@gmail.com",
	}
	ts.assert(req, "error_update_time_in_past_site_group_demand")
}
