package api

import (
	"context"
	"testing"
	"time"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/api"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/integrationtest"
)

type getDailyForecastSuite struct {
	tests.TestSuite
	faker *faker.Faker
	*gorm.DB
}

func TestGetDailyForecast(t *testing.T) {
	ts := &getDailyForecastSuite{}
	db, svsLog := ts.InitIntegrationTestMain(t, gormDb, logService)
	defer func(db *gorm.DB) {
		ts.tearDown(db)
	}(db)

	svc := &service.Service{
		SiteSkuDailyForecastRepository:   repository.NewSiteSkuDailyForecastRepository(db),
		SiteGroupDailyForecastRepository: repository.NewSiteGroupDailyForecastRepository(db),
		SiteSkuDemandRepository:          repository.NewSiteSkuDemandRepository(db),
		SiteGroupDemandRepository:        repository.NewSiteGroupDemandRepository(db),
		Log:                              svsLog,
		DB:                               db,
	}

	conn, server := ts.InitServerAndClient(t, svc)

	ts.faker = &faker.Faker{DB: db}
	ts.DB = db
	ts.tearDown(db)
	ts.Context = context.Background()
	suite.Run(t, ts)
	defer func() {
		err := conn.Close()
		if err != nil {
			svsLog.Error(err, "could not close connection")
		}
		server.Stop()
	}()
}

func (ts *getDailyForecastSuite) tearDown(db *gorm.DB) {
	integrationtest.TruncateDatabases(db,
		model.SiteSkuDailyForecast{}.TableName(),
		model.SiteGroupDailyForecast{}.TableName(),
		model.SiteSkuDemand{}.TableName(),
		model.SiteGroupDemand{}.TableName(),
	)
}

func (ts *getDailyForecastSuite) assertSuccess(req *api.GetDailyForecastRequest, want string) {
	res, err := ts.DemandPlanningClient.GetDailyForecast(ts.Context, req)
	assert.Nil(ts.T(), err)
	assert.NotNil(ts.T(), res)
	goldie.New(ts.T()).AssertJson(ts.T(), want, res)
}

func (ts *getDailyForecastSuite) TestSuccess_WithIsSkuDemand() {
	defer ts.tearDown(ts.DB)
	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		Id:          1,
		SellerId:    1,
		Sku:         "sku1",
		SiteId:      1,
		MonthOfYear: "2023-12",
		Forecast:    helper.Float64ToSqlFloat64(float64(310)),
	}, true)
	time.Sleep(time.Second * 1)

	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		SellerId:        1,
		SiteSkuDemandId: 1,
		Day:             10,
		Forecast:        float64(10),
	}, true)
	ts.faker.SiteSkuDailyForecast(&model.SiteSkuDailyForecast{
		SellerId:        1,
		SiteSkuDemandId: 1,
		Day:             20,
		Forecast:        float64(20),
	}, true)

	req := &api.GetDailyForecastRequest{
		SellerId:      1,
		GroupDemandId: 1,
		IsSkuDemand:   helper.BoolToProtoBool(true),
	}

	ts.assertSuccess(req, "get_daily_forecast/happy_case_with_is_sku_demand")
}

func (ts *getDailyForecastSuite) TestSuccess_WithoutIsSkuDemand() {
	defer ts.tearDown(ts.DB)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:            1,
		SellerId:      1,
		GroupKey:      "1/2/3",
		GroupBy:       "category",
		SiteId:        1,
		MonthOfYear:   "2023-12",
		Forecast:      helper.Float64ToSqlFloat64(float64(310)),
		IsLatestGroup: false,
	}, true)
	time.Sleep(time.Second * 1)

	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		SellerId:          1,
		SiteGroupDemandId: 1,
		Day:               10,
		Forecast:          float64(10),
	}, true)
	ts.faker.SiteGroupDailyForecast(&model.SiteGroupDailyForecast{
		SellerId:          1,
		SiteGroupDemandId: 1,
		Day:               20,
		Forecast:          float64(20),
	}, true)

	req := &api.GetDailyForecastRequest{
		SellerId:      1,
		GroupDemandId: 1,
		IsSkuDemand:   helper.BoolToProtoBool(false),
	}

	ts.assertSuccess(req, "get_daily_forecast/happy_case_without_is_sku_demand")
}
