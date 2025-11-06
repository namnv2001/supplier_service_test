package consumer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize/v3"
	"github.com/gogo/protobuf/types"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	exportServiceApi "go.tekoapis.com/tekone/app/aggregator/export-service/api"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/config"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/adapter/fileservice"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/adapter/whcentral"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/consumer/exports"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	whCentralApi "go.tekoapis.com/tekone/app/warehouse/central-service/api"
	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/test/monkey"
)

var isUpdateForecast = flag.Bool("update-forecast-excel", false, "is update or not")

type exportForecastTestCase struct {
	tests.TestSuite
	faker         *faker.Faker
	db            *gorm.DB
	worker        *exports.Worker
	mapResultFile map[string]*excelize.File
	sellerId      int32
	isGenNewFile  bool
}

func TestService_ExportForecast(t *testing.T) {
	ts := &exportForecastTestCase{}
	db, serviceLog := ts.InitIntegrationTestMain(t, gormDb, logService)
	ts.db = db
	ts.sellerId = 1
	ts.mapResultFile = make(map[string]*excelize.File)
	ts.faker = &faker.Faker{DB: db}

	defer func() {
		defer monkey.UnpatchAll()
		ts.tearDown()
	}()

	if isUpdateForecast != nil {
		ts.isGenNewFile = *isUpdateForecast
	}

	fileServiceClient := &fileservice.FilesClientImpl{}
	monkey.PatchInstanceMethod(reflect.TypeOf(fileServiceClient), "UploadDoc",
		func(c *fileservice.FilesClientImpl, filePath string) (*types.StringValue, error) {
			fileReader, err := os.ReadFile(filePath)
			assert.Nil(ts.T(), err)

			file, err := excelize.OpenReader(bytes.NewReader(fileReader))
			assert.Nil(ts.T(), err)
			ts.mapResultFile[filePath] = file
			return helper.StringToProtoString(filePath), nil
		})

	whCentralClient := &whcentral.ImplClient{}
	monkey.PatchInstanceMethod(reflect.TypeOf(whCentralClient), "GetMapSiteId2SiteInfo",
		func(client *whcentral.ImplClient, ctx context.Context, sellerId int32, siteIds []int32) (mapSiteCodeById map[int32]*whCentralApi.Site, err error) {
			return map[int32]*whCentralApi.Site{
				1: {
					Id:   1,
					Code: "site 1",
				},
				2: {
					Id:   2,
					Code: "site 2",
				},
				3: {
					Id:   3,
					Code: "site 3",
				},
			}, nil
		})

	svc := &service.Service{
		Config: &config.Config{
			MaxRetryUpload:      5,
			RetryUploadInterval: 5,
		},
		Log:                               serviceLog,
		DB:                                db,
		WHCentralClient:                   whCentralClient,
		FileService:                       fileServiceClient,
		SiteGroupDemandRepository:         repository.NewSiteGroupDemandRepository(db),
		MonthlyCategoryRepository:         repository.NewMonthlyCategoryRepository(db),
		MonthlyVariantAttributeRepository: repository.NewMonthlyVariantAttributeRepository(db),
	}
	ts.worker = exports.NewWorker(svc)
	ts.tearDown()
	suite.Run(t, ts)
}

func (ts *exportForecastTestCase) tearDown() {
	integrationtest.TruncateDatabases(ts.db,
		model.SiteGroupDemand{}.TableName(),
		model.MonthlyCategory{}.TableName(),
		model.MonthlyVariantAttribute{}.TableName(),
	)
}

func (ts *exportForecastTestCase) assert(exportReqPayload *exportServiceApi.PayloadDPExportForecast, wantFile string) {
	defer func() {
		ts.tearDown()
	}()

	payload, err := json.Marshal(exportReqPayload)
	assert.Nil(ts.T(), err)

	url, err := ts.worker.ExportForecast(context.Background(), &exportServiceApi.ExportEvent{
		SellerId: int64(ts.sellerId),
		Status:   "open",
		Payload:  string(payload),
	})
	assert.Nil(ts.T(), err)

	prefixPath := "export_forecast"
	if ts.isGenNewFile {
		err := ts.mapResultFile[url].SaveAs(fmt.Sprintf("testdata/%s/%s.xlsx", prefixPath, wantFile))
		if err != nil {
			return
		}
	}
	fileReader, err := os.ReadFile(fmt.Sprintf("testdata/%s/%s.xlsx", prefixPath, wantFile))
	assert.Nil(ts.T(), err)
	expected, err := excelize.OpenReader(bytes.NewReader(fileReader))
	assert.Nil(ts.T(), err)
	actual := ts.mapResultFile[url]
	cols, err := actual.GetRows(exports.ExportForecastSheetName)
	assert.Nil(ts.T(), err)
	goldie.New(ts.T()).AssertJson(ts.T(), fmt.Sprintf("%s/%s", prefixPath, wantFile), cols)

	for row := 1; row <= len(cols); row++ {
		for col := 0; col < len(cols[row-1]); col++ {
			colName := fmt.Sprintf("%c", col+'A')
			rs1, err := helper.GetCell(colName, row, expected, exports.ExportForecastSheetName)
			assert.Nil(ts.T(), err)
			rs2, err := helper.GetCell(colName, row, actual, exports.ExportForecastSheetName)
			assert.Nil(ts.T(), err)
			assert.Equal(ts.T(), rs1, rs2, helper.GetAxis(colName, row))
		}
	}
}

func (ts *exportForecastTestCase) initDefaultData() {
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          1,
		SellerId:    ts.sellerId,
		SiteId:      1,
		MonthOfYear: "2023-06",
		Budget: sql.NullFloat64{
			Float64: 1.2,
			Valid:   true,
		},
		Forecast: sql.NullFloat64{
			Float64: 2.3,
			Valid:   true,
		},
		ActualSaleValue: sql.NullFloat64{
			Float64: 3.4,
			Valid:   true,
		},
		NumberOfSkus: 5,
		GroupKey:     "1/2/3/4",
		GroupBy:      model.SiteGroupDemandGroupByCategory,
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          2,
		SellerId:    ts.sellerId,
		SiteId:      1,
		MonthOfYear: "2023-06",
		Budget: sql.NullFloat64{
			Float64: 1.3,
			Valid:   true,
		},
		Forecast: sql.NullFloat64{
			Float64: 2.4,
			Valid:   true,
		},
		ActualSaleValue: sql.NullFloat64{
			Float64: 3.5,
			Valid:   true,
		},
		NumberOfSkus: 6,
		GroupKey:     "2/2/3/4",
		GroupBy:      model.SiteGroupDemandGroupBySegment,
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          3,
		SellerId:    ts.sellerId,
		SiteId:      2,
		MonthOfYear: "2023-06",
		Budget: sql.NullFloat64{
			Float64: 1.4,
			Valid:   true,
		},
		Forecast: sql.NullFloat64{
			Float64: 2.5,
			Valid:   true,
		},
		ActualSaleValue: sql.NullFloat64{
			Float64: 3.6,
			Valid:   true,
		},
		NumberOfSkus: 7,
		GroupKey:     "1/2/3",
		GroupBy:      model.SiteGroupDemandGroupByCategory,
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          4,
		SellerId:    ts.sellerId,
		SiteId:      1,
		MonthOfYear: "2023-05",
		Budget: sql.NullFloat64{
			Float64: 1.5,
			Valid:   true,
		},
		Forecast: sql.NullFloat64{
			Float64: 2.6,
			Valid:   true,
		},
		ActualSaleValue: sql.NullFloat64{
			Float64: 3.7,
			Valid:   true,
		},
		NumberOfSkus: 8,
		GroupKey:     "1/2/3/4",
		GroupBy:      model.SiteGroupDemandGroupByCategory,
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          5,
		SellerId:    ts.sellerId,
		SiteId:      1,
		MonthOfYear: "2023-06",
		Budget: sql.NullFloat64{
			Float64: 1.6,
			Valid:   true,
		},
		ActualSaleValue: sql.NullFloat64{
			Float64: 3.8,
			Valid:   true,
		},
		NumberOfSkus: 9,
		GroupKey:     "1/1/2/3/4",
		GroupBy:      model.SiteGroupDemandGroupBySegment,
	}, true)
	ts.faker.SiteGroupDemand(&model.SiteGroupDemand{
		Id:          6,
		SellerId:    2,
		SiteId:      1,
		MonthOfYear: "2023-06",
		Budget: sql.NullFloat64{
			Float64: 1.2,
			Valid:   true,
		},
		Forecast: sql.NullFloat64{
			Float64: 2.3,
			Valid:   true,
		},
		ActualSaleValue: sql.NullFloat64{
			Float64: 3.4,
			Valid:   true,
		},
		NumberOfSkus: 5,
		GroupKey:     "1/2/3/4",
		GroupBy:      model.SiteGroupDemandGroupByCategory,
	}, true)

	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    ts.sellerId,
		MonthOfYear: "2023-06",
		CategoryId:  1,
		Code:        "Cate1",
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    ts.sellerId,
		MonthOfYear: "2023-06",
		CategoryId:  2,
		Code:        "Cate2",
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    ts.sellerId,
		MonthOfYear: "2023-06",
		CategoryId:  3,
		Code:        "Cate3",
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    ts.sellerId,
		MonthOfYear: "2023-06",
		CategoryId:  4,
		Code:        "Cate4",
	}, true)

	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 1,
		SellerId:         ts.sellerId,
		Sku:              "sku 1",
		AttributeId:      1,
		AttributeCode:    "attr 1",
		Value:            "Attribute Option 1",
		DisplayValue:     "Attribute Option 1",
		MonthOfYear:      "2023-06",
	}, true)
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 2,
		SellerId:         ts.sellerId,
		Sku:              "sku2",
		AttributeId:      2,
		AttributeCode:    "attr 2",
		Value:            "Attribute Option 2",
		DisplayValue:     "Attribute Option 2",
		MonthOfYear:      "2023-06",
	}, true)
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 3,
		SellerId:         ts.sellerId,
		Sku:              "sku3",
		AttributeId:      3,
		AttributeCode:    "attr 3",
		Value:            "Attribute Option 3",
		DisplayValue:     "Attribute Option 3",
		MonthOfYear:      "2023-06",
	}, true)
	ts.faker.MonthlyVariantAttribute(&model.MonthlyVariantAttribute{
		AttributeValueId: 4,
		SellerId:         ts.sellerId,
		Sku:              "sku4",
		AttributeId:      4,
		AttributeCode:    "attr 4",
		Value:            "Attribute Option 4",
		DisplayValue:     "Attribute Option 4",
		MonthOfYear:      "2023-06",
	}, true)
}

func (ts *exportForecastTestCase) Test200_GroupByCate_Site_1_Month_6_ReturnSuccess() {
	ts.initDefaultData()
	exportReqPayload := &exportServiceApi.PayloadDPExportForecast{
		SiteId:        1,
		RequestedDate: int32(time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC).Unix()),
		GroupBy:       "category",
	}

	ts.assert(exportReqPayload, "req_export_forecast_cate_site_1_month_6")
}

func (ts *exportForecastTestCase) Test200_GroupByCate_Site_0_Month_6_ReturnSuccess() {
	ts.initDefaultData()
	exportReqPayload := &exportServiceApi.PayloadDPExportForecast{
		SiteId:        0,
		RequestedDate: int32(time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC).Unix()),
		GroupBy:       "category",
	}

	ts.assert(exportReqPayload, "req_export_forecast_cate_site_0_month_6")
}

func (ts *exportForecastTestCase) Test200_GroupByCate_Site_1_Month_6_Cate_1_ReturnSuccess() {
	ts.initDefaultData()
	exportReqPayload := &exportServiceApi.PayloadDPExportForecast{
		CategoryId:    &types.Int32Value{Value: 1},
		SiteId:        1,
		RequestedDate: int32(time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC).Unix()),
		GroupBy:       "category",
	}

	ts.assert(exportReqPayload, "req_export_forecast_cate_site_1_month_6_cate_1")
}

func (ts *exportForecastTestCase) Test200_GroupBySeg_Site_1_Month_6_ReturnSuccess() {
	ts.initDefaultData()
	exportReqPayload := &exportServiceApi.PayloadDPExportForecast{
		SiteId:        1,
		RequestedDate: int32(time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC).Unix()),
		GroupBy:       "segment",
	}

	ts.assert(exportReqPayload, "req_export_forecast_seg_site_1_month_6")
}

func (ts *exportForecastTestCase) Test200_GroupBySeg_Site_0_Month_6_ReturnSuccess() {
	ts.initDefaultData()
	exportReqPayload := &exportServiceApi.PayloadDPExportForecast{
		SiteId:        0,
		RequestedDate: int32(time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC).Unix()),
		GroupBy:       "segment",
	}

	ts.assert(exportReqPayload, "req_export_forecast_seg_site_0_month_6")
}

func (ts *exportForecastTestCase) Test200_GroupBySeg_Site_1_Month_6_Cate_1_ReturnSuccess() {
	ts.initDefaultData()
	exportReqPayload := &exportServiceApi.PayloadDPExportForecast{
		CategoryId:    &types.Int32Value{Value: 1},
		SiteId:        1,
		RequestedDate: int32(time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC).Unix()),
		GroupBy:       "segment",
	}

	ts.assert(exportReqPayload, "req_export_forecast_seg_site_1_month_6_cate_1")
}
