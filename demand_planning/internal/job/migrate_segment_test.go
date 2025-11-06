package job

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize/v3"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	catalogGrpc "go.tekoapis.com/tekone/app/catalog/api"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/config"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/adapter/catalog"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/adapter/google_sheet"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/jobs/migrate_segment"
	mockCatalog "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/catalog"
	mockCatalogGrpc "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/catalog_grpc"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type migrateSegmentTest struct {
	tests.TestSuite
	faker      *faker.Faker
	db         *gorm.DB
	fixedTime  time.Time
	sellerId   int32
	job        *migrate_segment.MigrateSegmentJobImpl
	excelFile  *excelize.File
	getFileErr error
}

func TestMigrateSegment(t *testing.T) {
	ts := &migrateSegmentTest{}
	ts.Context = context.Background()
	ts.sellerId = 1
	db, _ := ts.InitIntegrationTestMain(t, gormDb, logService)
	ts.db = db
	ts.faker = &faker.Faker{DB: db}
	ts.fixedTime = time.Date(2022, time.November, 1, 1, 0, 0, 0, time.UTC)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal("Error")
	}
	googleSheetId := "googleSheetId"
	catalogClient := &mockCatalog.ClientAdapter{}
	catalogClient.Mock.On("GetAllAttributes", mock.Anything, mock.Anything).
		Return([]*catalog.Attribute{
			{
				Id:          1,
				Name:        "attribute 1",
				DisplayName: "Attribute 1",
				ValueType:   "selection",
				Code:        "ATT1",
				Description: "Attribute 1",
			},
			{
				Id:          2,
				Name:        "attribute 2",
				DisplayName: "Attribute 2",
				ValueType:   "selection",
				Code:        "ATT2",
				Description: "Attribute 2",
			},
		}, nil)
	catalogGrpcClient := &mockCatalogGrpc.ClientAdapter{}
	catalogGrpcClient.Mock.On("GetAllCategoryByFilter", mock.Anything, mock.Anything).
		Return([]*catalogGrpc.BaseCategoryInfo{
			{
				Id:       1,
				Code:     "CATE1",
				Name:     "category 1",
				IsActive: true,
				SellerId: ts.sellerId,
			},
			{
				Id:       2,
				Code:     "CATE2",
				Name:     "category 2",
				IsActive: true,
				SellerId: ts.sellerId,
			},
		}, nil)

	googleSheetClient := &google_sheet.GoogleSheetImpl{}
	monkey.PatchInstanceMethod(reflect.TypeOf(googleSheetClient), "GetXlsxFile",
		func(c *google_sheet.GoogleSheetImpl, ctx context.Context, googleSheetId string) (*excelize.File, error) {
			return ts.excelFile, ts.getFileErr
		})

	monkey.Patch(time.Now, func() time.Time {
		return ts.fixedTime
	})

	ts.job = &migrate_segment.MigrateSegmentJobImpl{
		Cfg:                      *cfg,
		Db:                       db,
		MonthlySegmentRepository: repository.NewMonthlySegmentRepository(db),
		CatalogGrpcClient:        catalogGrpcClient,
		CatalogClient:            catalogClient,
		GoogleSheetClient:        googleSheetClient,
		GoogleSheetId:            googleSheetId,
		Logger:                   ctxzap.Extract(ts.Context),
		SellerId:                 ts.sellerId,
		GoogleSheetName:          "Sheet1",
	}
	defer func() {
		ts.tearDown()
		monkey.UnpatchAll()
	}()

	suite.Run(t, ts)
}

func (ts *migrateSegmentTest) tearDown() {
	integrationtest.TruncateDatabases(ts.db, []string{model.MonthlySegment{}.TableName()}...)
}

func (ts *migrateSegmentTest) assertMonthlySegment(wantFile string) {
	defer ts.tearDown()

	var monthlySegments []*model.MonthlySegment
	err := ts.db.Table(model.MonthlySegment{}.TableName()).Find(&monthlySegments).Order("seller_id asc, category_id asc, attribute_id asc, month_of_year asc").Error
	assert.Nil(ts.T(), err)
	for _, data := range monthlySegments {
		data.CreatedAt = ts.fixedTime
		data.UpdatedAt = ts.fixedTime
	}
	goldie.New(ts.T()).AssertJson(ts.T(), fmt.Sprintf("migrate_segment/%s", wantFile), monthlySegments)
}

func (ts *migrateSegmentTest) TestHappyCase() {
	fileReader, err := os.ReadFile("test_data/migrate_segment/happy_case.xlsx")
	assert.Nil(ts.T(), err)
	ts.excelFile, ts.getFileErr = excelize.OpenReader(bytes.NewReader(fileReader))

	err = ts.job.Run(ts.Context)
	assert.Nil(ts.T(), err)

	ts.assertMonthlySegment("happy_case")
}

func (ts *migrateSegmentTest) TestCase_Not_Found_Category_And_Attribute() {
	fileReader, err := os.ReadFile("test_data/migrate_segment/not_found_category_and_attribute.xlsx")
	assert.Nil(ts.T(), err)
	ts.excelFile, ts.getFileErr = excelize.OpenReader(bytes.NewReader(fileReader))

	err = ts.job.Run(ts.Context)
	assert.Nil(ts.T(), err)

	ts.assertMonthlySegment("not_found_category_and_attribute")
}

func (ts *migrateSegmentTest) TestCase_Upsert() {
	ts.faker.MonthlySegment(&model.MonthlySegment{
		SellerId:    1,
		AttributeId: 1,
		CategoryId:  1,
		Level:       10,
		MonthOfYear: "2022-11",
	}, true)
	fileReader, err := os.ReadFile("test_data/migrate_segment/upsert.xlsx")
	assert.Nil(ts.T(), err)
	ts.excelFile, ts.getFileErr = excelize.OpenReader(bytes.NewReader(fileReader))

	err = ts.job.Run(ts.Context)
	assert.Nil(ts.T(), err)

	ts.assertMonthlySegment("upsert")
}

func (ts *migrateSegmentTest) TestCase_RowGroupLength() {
	ts.faker.MonthlySegment(&model.MonthlySegment{
		SellerId:    1,
		AttributeId: 1,
		CategoryId:  1,
		Level:       10,
		MonthOfYear: "2022-11",
	}, true)
	fileReader, err := os.ReadFile("test_data/migrate_segment/row_group_length.xlsx")
	assert.Nil(ts.T(), err)
	ts.excelFile, ts.getFileErr = excelize.OpenReader(bytes.NewReader(fileReader))

	err = ts.job.Run(ts.Context)
	assert.Nil(ts.T(), err)

	ts.assertMonthlySegment("row_group_length")
}

func (ts *migrateSegmentTest) TestCase_Duplicate_Category() {
	fileReader, err := os.ReadFile("test_data/migrate_segment/duplicate_category.xlsx")
	assert.Nil(ts.T(), err)
	ts.excelFile, ts.getFileErr = excelize.OpenReader(bytes.NewReader(fileReader))

	err = ts.job.Run(ts.Context)
	assert.Nil(ts.T(), err)

	ts.assertMonthlySegment("duplicate_category")
}
