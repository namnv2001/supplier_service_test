package consumer

import (
	"bytes"
	"context"
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
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/adapter/export_service"
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

var isUpdate = flag.Bool("update-excel", false, "is update or not")

type exportBudgetTestCase struct {
	tests.TestSuite
	faker         *faker.Faker
	db            *gorm.DB
	worker        *exports.Worker
	mapResultFile map[string]*excelize.File
	fixedTime     time.Time
	sellerId      int32
	isGenNewFile  bool
}

func TestService_ExportBudget(t *testing.T) {
	ts := &exportBudgetTestCase{}
	db, serviceLog := ts.InitIntegrationTestMain(t, gormDb, logService)
	ts.db = db
	ts.sellerId = 3
	ts.fixedTime = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	ts.mapResultFile = make(map[string]*excelize.File)
	ts.faker = &faker.Faker{DB: db}

	defer func() {
		defer monkey.UnpatchAll()
		ts.tearDown()
	}()

	if isUpdate != nil {
		ts.isGenNewFile = *isUpdate
	}

	mockExportService := &export_service.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(mockExportService), "GetExportRequest",
		func(c *export_service.Client, ctx context.Context, reqExport *exportServiceApi.GetExportRequestRequest) (*exportServiceApi.GetExportRequestResponse, error) {
			return &exportServiceApi.GetExportRequestResponse{
				Data: &exportServiceApi.GetExportRequestResponse_Data{
					ExportRequest: &exportServiceApi.GetExportRequestResponse_Data_ExportRequest{
						RequestId: "request-1",
						Status:    "open",
					},
				},
			}, nil
		})
	monkey.PatchInstanceMethod(reflect.TypeOf(mockExportService), "UpdateExportRequest",
		func(c *export_service.Client, ctx context.Context, reqExport *exportServiceApi.UpdateExportRequestRequest) (*exportServiceApi.UpdateExportRequestResponse, error) {
			return &exportServiceApi.UpdateExportRequestResponse{}, nil
		})

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
		Log:                       serviceLog,
		DB:                        db,
		WHCentralClient:           whCentralClient,
		ExportServiceClient:       mockExportService,
		FileService:               fileServiceClient,
		OriginalBudgetRepository:  repository.NewOriginalBudgetRepository(db),
		MonthlyCategoryRepository: repository.NewMonthlyCategoryRepository(db),
	}
	ts.worker = exports.NewWorker(svc)
	ts.tearDown()
	suite.Run(t, ts)
}

func (ts *exportBudgetTestCase) tearDown() {
	integrationtest.TruncateDatabases(ts.db,
		model.OriginalBudget{}.TableName(),
		model.MonthlyCategory{}.TableName(),
	)
}

func (ts *exportBudgetTestCase) assert(exportReqPayload *exportServiceApi.PayloadDPExportBudget, wantFile string) {
	defer func() {
		ts.tearDown()
	}()

	payload, err := json.Marshal(exportReqPayload)
	assert.Nil(ts.T(), err)

	url, err := ts.worker.ExportBudget(context.Background(), &exportServiceApi.ExportEvent{
		SellerId: int64(ts.sellerId),
		Status:   "open",
		Payload:  string(payload),
	})
	assert.Nil(ts.T(), err)

	prefixPath := "export_budget"
	if ts.isGenNewFile {
		ts.mapResultFile[url].SaveAs(fmt.Sprintf("testdata/%s/%s.xlsx", prefixPath, wantFile))
	}
	fileReader, err := os.ReadFile(fmt.Sprintf("testdata/%s/%s.xlsx", prefixPath, wantFile))
	assert.Nil(ts.T(), err)
	expected, err := excelize.OpenReader(bytes.NewReader(fileReader))
	assert.Nil(ts.T(), err)
	actual := ts.mapResultFile[url]
	rows, err := actual.GetRows(exports.ExportBudgetSheetName)
	assert.Nil(ts.T(), err)
	goldie.New(ts.T()).AssertJson(ts.T(), fmt.Sprintf("%s/%s", prefixPath, wantFile), rows)

	for row := 1; row <= len(rows); row++ {
		for col := 'A'; col < 'Z'; col++ {
			colName := fmt.Sprintf("%c", col)
			rs1, err := helper.GetCell(colName, row, expected, exports.ExportBudgetSheetName)
			assert.Nil(ts.T(), err)
			rs2, err := helper.GetCell(colName, row, actual, exports.ExportBudgetSheetName)
			assert.Nil(ts.T(), err)
			assert.Equal(ts.T(), rs1, rs2, helper.GetAxis(colName, row))
		}
	}
}

func (ts *exportBudgetTestCase) Test200_ReqExportMultiYear_ReturnSuccess() {
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          1,
		SellerId:    ts.sellerId,
		SiteId:      1,
		CategoryId:  1,
		MonthOfYear: "2021-06",
		Budget:      helper.ToNullFloat64(10010100.123),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          2,
		SellerId:    ts.sellerId,
		SiteId:      1,
		CategoryId:  1,
		MonthOfYear: "2022-02",
		Budget:      helper.ToNullFloat64(200200200),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          3,
		SellerId:    ts.sellerId,
		SiteId:      1,
		CategoryId:  1,
		MonthOfYear: "2023-01",
		Budget:      helper.ToNullFloat64(500.2002),
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		Id:         1,
		SellerId:   ts.sellerId,
		CategoryId: 1,
		Code:       "category code 1",
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		Id:         2,
		SellerId:   ts.sellerId,
		CategoryId: 2,
		Code:       "category code 2",
	}, true)

	exportReqPayload := &exportServiceApi.PayloadDPExportBudget{
		CategoryId: 1,
		StartDate:  int32(time.Date(2021, 5, 1, 0, 0, 0, 0, time.UTC).Unix()),
		EndDate:    int32(time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC).Unix()),
	}

	ts.assert(exportReqPayload, "req_export_multi_year_then_return_success")
}

func (ts *exportBudgetTestCase) Test200_ReqExportAHalfYear_ReturnSuccess() {
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          1,
		SellerId:    ts.sellerId,
		SiteId:      1,
		CategoryId:  1,
		MonthOfYear: "2021-01",
		Budget:      helper.ToNullFloat64(100),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          2,
		SellerId:    ts.sellerId,
		SiteId:      1,
		CategoryId:  1,
		MonthOfYear: "2021-02",
		Budget:      helper.ToNullFloat64(200),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          3,
		SellerId:    ts.sellerId,
		SiteId:      1,
		CategoryId:  1,
		MonthOfYear: "2021-10",
		Budget:      helper.ToNullFloat64(500),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          4,
		SellerId:    ts.sellerId,
		SiteId:      1,
		CategoryId:  1,
		MonthOfYear: "2021-05",
		Budget:      helper.ToNullFloat64(100),
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		Id:         1,
		SellerId:   ts.sellerId,
		CategoryId: 1,
		Code:       "category code 1",
	}, true)

	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          5,
		SellerId:    ts.sellerId,
		SiteId:      2,
		CategoryId:  1,
		MonthOfYear: "2021-04",
		Budget:      helper.ToNullFloat64(100),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          6,
		SellerId:    ts.sellerId,
		SiteId:      2,
		CategoryId:  1,
		MonthOfYear: "2021-03",
		Budget:      helper.ToNullFloat64(200),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          7,
		SellerId:    ts.sellerId,
		SiteId:      2,
		CategoryId:  1,
		MonthOfYear: "2021-09",
		Budget:      helper.ToNullFloat64(500),
	}, true)
	ts.faker.OriginalBudget(&model.OriginalBudget{
		Id:          8,
		SellerId:    ts.sellerId,
		SiteId:      2,
		CategoryId:  1,
		MonthOfYear: "2021-07",
		Budget:      helper.ToNullFloat64(100),
	}, true)

	exportReqPayload := &exportServiceApi.PayloadDPExportBudget{
		CategoryId: 1,
		StartDate:  int32(time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC).Unix()),
		EndDate:    int32(time.Date(2021, 10, 1, 0, 0, 0, 0, time.UTC).Unix()),
	}

	ts.assert(exportReqPayload, "req_export_a_half_year_then_return_success")
}

func (ts *exportBudgetTestCase) Test200_ReqExportButDbEmptyData_ReturnSuccess() {
	exportReqPayload := &exportServiceApi.PayloadDPExportBudget{
		CategoryId: 1,
		StartDate:  int32(time.Date(2021, 5, 1, 0, 0, 0, 0, time.UTC).Unix()),
		EndDate:    int32(time.Date(2021, 8, 1, 0, 0, 0, 0, time.UTC).Unix()),
	}

	ts.assert(exportReqPayload, "req_export_but_db_empty_data_then_return_success")
}
