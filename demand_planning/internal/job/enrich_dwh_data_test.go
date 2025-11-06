package job

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/minio/minio-go/v7"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/config"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/jobs/enrich_dwh_data"
	mockWHCentral "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/whcentral"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type enrichDWHDataTestSuite struct {
	tests.TestSuite
	faker              *faker.Faker
	db                 *gorm.DB
	fixedTime          time.Time
	job                *enrich_dwh_data.EnrichDWHDataJobImpl
	downloadMinIOError error
}

type assertObj struct {
	SiteSkuDemands   []*model.SiteSkuDemand
	SiteGroupDemands []*model.SiteGroupDemand
}

func TestJob_EnrichDWHData(t *testing.T) {
	ts := &enrichDWHDataTestSuite{}
	db, _ := ts.InitIntegrationTestMain(t, gormDb, logService)
	ts.db = db
	ts.fixedTime = time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)
	ts.faker = &faker.Faker{DB: db}

	ts.tearDown()

	defer func() {
		ts.tearDown()
		monkey.UnpatchAll()
	}()
	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		t.Fatal("Error")
	}

	cfg.ParquetData.ProductDim.File = "parquet/vnshop_product"
	cfg.ParquetData.ShopDim.File = "parquet/shop"
	cfg.ParquetData.SkuShopDaily.File = "parquet/sku_shop_daily"
	cfg.ParquetData.SkuMerchantMonthly.File = "parquet/sku_merchant_monthly"
	cfg.ParquetData.SkuMerchantShopMonthly.File = "parquet/sku_merchant_shop_monthly"

	whCentralClient := &mockWHCentral.ClientAdapter{}
	whCentralClient.Mock.On("GetMapSiteCode2SiteId", mock.Anything, int32(1)).
		Return(map[string]int32{
			"SITE01": 1,
			"SITE02": 2,
			"SITE03": 3,
			"SITE04": 4,
			"SITE05": 5,
			"SITE06": 6,
			"SITE07": 7,
			"SITE08": 8,
		}, nil).
		On("GetMapSiteCode2SiteId", mock.Anything, int32(2)).
		Return(
			map[string]int32{
				"SITE09": 9,
				"SITE10": 10,
			}, nil)

	monkey.Patch(helper.DownloadMinio, func(ctx context.Context, log *zap.Logger, config helper.MinIOConfig, bucket string, q minio.ListObjectsOptions, savedFileName string, optional helper.MinIOReadOptional) (fileAttrs []helper.FileAttr, err error) {
		return nil, ts.downloadMinIOError
	})

	ts.job = &enrich_dwh_data.EnrichDWHDataJobImpl{
		Cfg:                                    *cfg,
		Db:                                     db,
		Logger:                                 ctxzap.Extract(ctx),
		SiteSkuDemandRepository:                repository.NewSiteSkuDemandRepository(db),
		SiteGroupDemandRepository:              repository.NewSiteGroupDemandRepository(db),
		SellerConfigRepository:                 repository.NewSellerConfigRepository(db),
		MonthlySkuCategoryMappingRepository:    repository.NewMonthlySkuCategoryMappingRepository(db),
		MonthlyCategoryRepository:              repository.NewMonthlyCategoryRepository(db),
		MonthlySkuSegmentPathMappingRepository: repository.NewMonthlySkuSegmentPathMappingRepository(db),

		WHCentralClient: whCentralClient,
		JobStartTime:    ts.fixedTime.Unix(),
	}

	suite.Run(t, ts)
}

func (ts *enrichDWHDataTestSuite) tearDown() {
	integrationtest.TruncateDatabases(ts.db,
		model.SiteSkuDemand{}.TableName(),
		model.SiteGroupDemand{}.TableName(),
		model.SellerConfig{}.TableName(),
		model.MonthlySkuCategoryMapping{}.TableName(),
		model.MonthlyCategory{}.TableName(),
		model.MonthlySkuSegmentPathMapping{}.TableName(),
	)
}

func (ts *enrichDWHDataTestSuite) setUp() {
	ts.faker.SellerConfig(&model.SellerConfig{
		SellerId:          1,
		UseDemandPlanning: true,
	}, true)
	ts.faker.SellerConfig(&model.SellerConfig{
		SellerId:          2,
		UseDemandPlanning: true,
	}, true)

	ts.faker.SiteSkuDemand(&model.SiteSkuDemand{
		SellerId:    1,
		Sku:         "sku01",
		SiteId:      1,
		MonthOfYear: "2023-10",
		Budget:      helper.Float64ToSqlFloat64(12893.23),
	}, true)

	monthOfYear := "2023-10"

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
		SellerId:    2,
		CategoryId:  4,
		Code:        "CATE4",
		Path:        helper.StringToSqlString("0"),
		Name:        "Category 4",
		MonthOfYear: monthOfYear,
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    2,
		CategoryId:  5,
		Code:        "CATE5",
		Path:        helper.StringToSqlString("4/5"),
		Name:        "Category 5",
		ParentId:    helper.Int32ToSqlInt32(4),
		MonthOfYear: monthOfYear,
	}, true)

	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku01",
		SellerSku:             "sellerSku1",
		Name:                  "sku name 1",
		LatestChildCategoryId: 2,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku02",
		SellerSku:             "sellerSku2",
		Name:                  "sku name 2",
		LatestChildCategoryId: 2,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku03",
		SellerSku:             "sellerSku3",
		Name:                  "sku name 3",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku04",
		SellerSku:             "sellerSku4",
		Name:                  "sku name 4",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku05",
		SellerSku:             "sellerSku5",
		Name:                  "sku name 5",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku06",
		SellerSku:             "sellerSku6",
		Name:                  "sku name 6",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku07",
		SellerSku:             "sellerSku7",
		Name:                  "sku name 7",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku08",
		SellerSku:             "sellerSku8",
		Name:                  "sku name 8",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              2,
		Sku:                   "sku09",
		SellerSku:             "sellerSku9",
		Name:                  "sku name 9",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              2,
		Sku:                   "sku10",
		SellerSku:             "sellerSku10",
		Name:                  "sku name 10",
		LatestChildCategoryId: 3,
		MonthOfYear:           monthOfYear,
	}, true)

	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku01",
		MasterCategoryId: 1,
		SegmentPath:      "1/2",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku02",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku03",
		MasterCategoryId: 1,
		SegmentPath:      "1/2",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku04",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku05",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku06",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku07",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         1,
		Sku:              "sku08",
		MasterCategoryId: 1,
		SegmentPath:      "1/3",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         2,
		Sku:              "sku09",
		MasterCategoryId: 4,
		SegmentPath:      "4/5",
		MonthOfYear:      monthOfYear,
	}, true)
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:         2,
		Sku:              "sku10",
		MasterCategoryId: 4,
		SegmentPath:      "4/5",
		MonthOfYear:      monthOfYear,
	}, true)
}

func (ts *enrichDWHDataTestSuite) Test_EnrichDWHDataJobImpl_RunBySeller() {
	ts.setUp()
	defer ts.tearDown()
	ts.job.Run(context.Background())

	var siteSkuDemands []*model.SiteSkuDemand
	err := ts.db.Table(model.SiteSkuDemand{}.TableName()).Order("month_of_year, sku, site_id").Find(&siteSkuDemands).Error
	if err != nil {
		ts.T().Fatal(err)
	}
	for _, item := range siteSkuDemands {
		item.Id = 0
		item.CreatedAt = ts.fixedTime
		item.UpdatedAt = ts.fixedTime
	}

	var siteGroupDemands []*model.SiteGroupDemand
	ts.db.Debug()
	err = ts.db.Table(model.SiteGroupDemand{}.TableName()).Order("month_of_year, group_by, group_key, site_id").Find(&siteGroupDemands).Error
	if err != nil {
		ts.T().Fatal(err)
	}
	for _, item := range siteGroupDemands {
		item.Id = 0
		item.CreatedAt = ts.fixedTime
		item.UpdatedAt = ts.fixedTime
	}

	goldie.New(ts.T()).AssertJson(ts.T(), "enrich_dwh_data/happy_case_enrich_dwh_data_successful", assertObj{
		SiteSkuDemands:   siteSkuDemands,
		SiteGroupDemands: siteGroupDemands,
	})
}

func (ts *enrichDWHDataTestSuite) Test_EnrichDWHDataJobImpl_RunBySeller_EnrichDataFromMinIOFailed() {
	ts.setUp()
	ts.downloadMinIOError = fmt.Errorf("error")
	defer func() {
		ts.downloadMinIOError = nil
		ts.tearDown()
	}()
	ts.job.Run(context.Background())

	var siteSkuDemands []*model.SiteSkuDemand
	err := ts.db.Table(model.SiteSkuDemand{}.TableName()).Order("month_of_year, sku, site_id").Find(&siteSkuDemands).Error
	if err != nil {
		ts.T().Fatal(err)
	}
	for _, item := range siteSkuDemands {
		item.Id = 0
		item.CreatedAt = ts.fixedTime
		item.UpdatedAt = ts.fixedTime
	}

	var siteGroupDemands []*model.SiteGroupDemand
	err = ts.db.Order("month_of_year, group_by, group_key, site_id").Find(&siteGroupDemands).Error
	if err != nil {
		ts.T().Fatal(err)
	}
	for _, item := range siteGroupDemands {
		item.Id = 0
		item.CreatedAt = ts.fixedTime
		item.UpdatedAt = ts.fixedTime
	}

	goldie.New(ts.T()).AssertJson(ts.T(), "enrich_dwh_data/happy_case_enrich_data_from_minio_failed", assertObj{
		SiteSkuDemands:   siteSkuDemands,
		SiteGroupDemands: siteGroupDemands,
	})
}
