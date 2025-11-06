package job

import (
	"context"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	catalogGrpc "go.tekoapis.com/tekone/app/catalog/api"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/config"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/adapter/catalog"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/jobs/enrich_catalog_data"
	mockCatalog "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/catalog"
	mockCatalogGrpc "go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/mocks/adapter/catalog_grpc"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/mocks/faker"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type enrichCatalogDataTestSuite struct {
	tests.TestSuite
	faker     *faker.Faker
	db        *gorm.DB
	fixedTime time.Time
	sellerId  int32
	job       *enrich_catalog_data.EnrichCatalogDataJobImpl
}

func TestJob_EnrichCatalogData(t *testing.T) {
	ts := &enrichCatalogDataTestSuite{}
	db, _ := ts.InitIntegrationTestMain(t, gormDb, logService)
	ts.db = db
	ts.sellerId = 3
	ts.fixedTime = time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)
	ts.faker = &faker.Faker{DB: db}

	ts.tearDown()
	ts.setUp()

	defer func() {
		ts.tearDown()
		monkey.UnpatchAll()
	}()

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
				Options: []catalog.AttributeOption{
					{
						Id:    1,
						Code:  "ATTOPT1",
						Value: "1,2,3",
					},
					{
						Id:    2,
						Code:  "ATTOPT2",
						Value: "Attribute Option 2",
					},
					{
						Id:    3,
						Code:  "ATTOPT3",
						Value: "att 1,att 2,att 3",
					},
				},
			},
			{
				Id:          2,
				Name:        "attribute 2",
				DisplayName: "Attribute 2",
				ValueType:   "multiple_select",
				Code:        "ATT2",
				Description: "Attribute 2",
				Options: []catalog.AttributeOption{
					{
						Id:    4,
						Code:  "ATTOPT4",
						Value: "123",
					},
					{
						Id:    5,
						Code:  "ATTOPT5",
						Value: "Attribute Option 5",
					},
					{
						Id:    6,
						Code:  "ATTOPT6",
						Value: "Attribute Option 6",
					},
				},
			},
			{
				Id:          3,
				Name:        "attribute 3",
				DisplayName: "Attribute 3",
				ValueType:   "selection",
				Code:        "ATT3",
				Description: "Attribute 3",
				Options: []catalog.AttributeOption{
					{
						Id:    7,
						Code:  "ATTOPT7",
						Value: "12.5",
					},
					{
						Id:    8,
						Code:  "ATTOPT8",
						Value: "Attribute Option 8",
					},
				},
			},
			{
				Id:          4,
				Name:        "attribute 4",
				DisplayName: "Attribute 4",
				ValueType:   "Text",
				Code:        "ATT4",
				Description: "Attribute 4",
			},
			{
				Id:          5,
				Name:        "attribute 5",
				DisplayName: "Attribute 5",
				ValueType:   "selection",
				Code:        "ATT5",
				Description: "Attribute 5",
				Options: []catalog.AttributeOption{
					{
						Id:    11,
						Code:  "ATTOPT11",
						Value: "Attribute Option 11",
					},
					{
						Id:    12,
						Code:  "ATTOPT12",
						Value: "Attribute Option 12",
					},
				},
			},
			{
				Id:          6,
				Name:        "attribute 6",
				DisplayName: "Attribute 6",
				ValueType:   "number",
				Code:        "ATT6",
				Description: "Attribute 6",
			},
		}, nil)
	catalogClient.Mock.On("GetAttributesForListVariants", mock.Anything, mock.Anything).
		Return([]catalog.Variant{
			{
				Id: 1,
				Attributes: []catalog.AttributeValue{
					{
						Id:                    1,
						Value:                 1,
						PlatformAttributeCode: "ATT1",
					},
					{
						Id:                    2,
						Value:                 []interface{}{4, 6},
						PlatformAttributeCode: "ATT2",
					},
					{
						Id:                    3,
						Value:                 7,
						PlatformAttributeCode: "ATT3",
					},
				},
			},
			{
				Id: 2,
				Attributes: []catalog.AttributeValue{
					{
						Id:                    1,
						Value:                 2,
						PlatformAttributeCode: "ATT1",
					},
					{
						Id:                    2,
						Value:                 []interface{}{6},
						PlatformAttributeCode: "ATT2",
					},
					{
						Id:                    3,
						Value:                 8,
						PlatformAttributeCode: "ATT3",
					},
				},
			},
			{
				Id: 3,
				Attributes: []catalog.AttributeValue{
					{
						Id:                    1,
						Value:                 3,
						PlatformAttributeCode: "ATT1",
					},
					{
						Id:                    2,
						Value:                 []interface{}{1, 2, 3},
						PlatformAttributeCode: "ATT2",
					},
					{
						Id:                    3,
						Value:                 8,
						PlatformAttributeCode: "ATT3",
					},
				},
			},
			{
				Id: 4,
				Attributes: []catalog.AttributeValue{
					{
						Id:                    4,
						Value:                 "Text value",
						PlatformAttributeCode: "ATT4",
					},
					{
						Id:                    2,
						Value:                 []interface{}{1, 3},
						PlatformAttributeCode: "ATT2",
					},
					{
						Id:                    3,
						Value:                 7,
						PlatformAttributeCode: "ATT3",
					},
				},
			},
			{
				Id: 5,
				Attributes: []catalog.AttributeValue{
					{
						Id:                    4,
						Value:                 "Attribute Option 9",
						PlatformAttributeCode: "ATT4",
					},
				},
			},
			{
				Id: 6,
				Attributes: []catalog.AttributeValue{
					{
						Id:                    5,
						Value:                 11,
						PlatformAttributeCode: "ATT5",
					},
					{
						Id:                    6,
						Value:                 3,
						PlatformAttributeCode: "ATT6",
					},
				},
			},
			{
				Id: 7,
				Attributes: []catalog.AttributeValue{
					{
						Id:                    5,
						Value:                 12,
						PlatformAttributeCode: "ATT5",
					},
					{
						Id:                    6,
						Value:                 17,
						PlatformAttributeCode: "ATT6",
					},
				},
			},
		}, nil)

	catalogGrpcClient := &mockCatalogGrpc.ClientAdapter{}
	catalogGrpcClient.Mock.On("GetAllSkusByFilter", mock.Anything, mock.Anything).
		Return([]*catalogGrpc.BaseProductInfo{
			{
				Id:           1,
				Sku:          "sku1",
				SellerSku:    "sellerSku1",
				Name:         "sku name 1",
				ProductName:  "name 1",
				SellerId:     1,
				UomRatio:     helper.Float64ToProtoDouble(1),
				UomName:      "uom",
				CategoryId:   1,
				CategoryName: "category 1",
				VariantId:    1,
				UomCode:      helper.StringToProtoString("uom"),
			},
			{
				Id:           2,
				Sku:          "sku2",
				SellerSku:    "sellerSku2",
				Name:         "sku name 2",
				ProductName:  "name 2",
				SellerId:     1,
				UomRatio:     helper.Float64ToProtoDouble(1),
				UomName:      "uom",
				CategoryId:   1,
				CategoryName: "category 1",
				VariantId:    2,
				UomCode:      helper.StringToProtoString("uom"),
			},
			{
				Id:           3,
				Sku:          "sku3",
				SellerSku:    "sellerSku3",
				Name:         "sku name 3",
				ProductName:  "name 3",
				SellerId:     1,
				UomRatio:     helper.Float64ToProtoDouble(1),
				UomName:      "uom",
				CategoryId:   1,
				CategoryName: "category 1",
				VariantId:    3,
				UomCode:      helper.StringToProtoString("uom"),
			},
			{
				Id:           4,
				Sku:          "sku4",
				SellerSku:    "sellerSku4",
				Name:         "sku name 4",
				ProductName:  "name 4",
				SellerId:     1,
				UomRatio:     helper.Float64ToProtoDouble(1),
				UomName:      "uom",
				CategoryId:   2,
				CategoryName: "category 2",
				VariantId:    4,
				UomCode:      helper.StringToProtoString("uom"),
			},
			{
				Id:           5,
				Sku:          "sku5",
				SellerSku:    "sellerSku5",
				Name:         "sku name 5",
				ProductName:  "name 5",
				SellerId:     1,
				UomRatio:     helper.Float64ToProtoDouble(1),
				UomName:      "uom",
				CategoryId:   2,
				CategoryName: "category 2",
				VariantId:    5,
				UomCode:      helper.StringToProtoString("uom"),
			},
			{
				Id:           6,
				Sku:          "sku6",
				SellerSku:    "sellerSku6",
				Name:         "sku name 6",
				ProductName:  "name 6",
				SellerId:     1,
				UomRatio:     helper.Float64ToProtoDouble(1),
				UomName:      "uom",
				CategoryId:   3,
				CategoryName: "category 3",
				VariantId:    6,
				UomCode:      helper.StringToProtoString("uom"),
			},
			{
				Id:           7,
				Sku:          "sku7",
				SellerSku:    "sellerSku7",
				Name:         "sku name 7",
				ProductName:  "name 7",
				SellerId:     1,
				UomRatio:     helper.Float64ToProtoDouble(1),
				UomName:      "uom",
				CategoryId:   3,
				CategoryName: "category 3",
				VariantId:    7,
				UomCode:      helper.StringToProtoString("uom"),
			},
		}, nil)
	catalogGrpcClient.Mock.On("GetAllCategoryByFilter", mock.Anything, mock.Anything).
		Return([]*catalogGrpc.BaseCategoryInfo{
			{
				Id:       7,
				Code:     "CATE7",
				Name:     "category 7",
				IsActive: true,
				SellerId: 1,
			},
			{
				Id:         4,
				Code:       "CATE4",
				Name:       "category 4",
				IsActive:   true,
				SellerId:   1,
				ParentId:   helper.Int32ToProtoInt32(7),
				ParentName: "category 7",
				Path:       "7/4",
			},
			{
				Id:         1,
				Code:       "CATE1",
				Name:       "category 1",
				IsActive:   true,
				SellerId:   1,
				ParentId:   helper.Int32ToProtoInt32(4),
				ParentName: "category 4",
				Path:       "7/4/1",
			},
			{
				Id:         2,
				Code:       "CATE2",
				Name:       "category 2",
				IsActive:   true,
				SellerId:   1,
				ParentId:   helper.Int32ToProtoInt32(4),
				ParentName: "category 4",
				Path:       "7/4/2",
			},
			{
				Id:         3,
				Code:       "CATE3",
				Name:       "category 3",
				IsActive:   true,
				SellerId:   1,
				ParentId:   helper.Int32ToProtoInt32(7),
				ParentName: "category 7",
				Path:       "7/3",
			},
		}, nil)

	monkey.Patch(helper.Today, func() time.Time {
		return ts.fixedTime
	})

	ctx := context.Background()
	ts.job = &enrich_catalog_data.EnrichCatalogDataJobImpl{
		Cfg:                                    config.Config{},
		Db:                                     db,
		Logger:                                 ctxzap.Extract(ctx),
		CatalogGrpcClient:                      catalogGrpcClient,
		CatalogClient:                          catalogClient,
		SellerConfigRepository:                 repository.NewSellerConfigRepository(db),
		MonthlyCategoryRepository:              repository.NewMonthlyCategoryRepository(db),
		MonthlyVariantAttributeRepository:      repository.NewMonthlyVariantAttributeRepository(db),
		MonthlySkuCategoryMappingRepository:    repository.NewMonthlySkuCategoryMappingRepository(db),
		MonthlySkuSegmentPathMappingRepository: repository.NewMonthlySkuSegmentPathMappingRepository(db),
		MonthlySegmentRepository:               repository.NewMonthlySegmentRepository(db),
	}

	suite.Run(t, ts)
}

func (ts *enrichCatalogDataTestSuite) tearDown() {
	integrationtest.TruncateDatabases(ts.db,
		model.MonthlyCategory{}.TableName(),
		model.MonthlySegment{}.TableName(),
		model.MonthlySkuCategoryMapping{}.TableName(),
		model.MonthlyVariantAttribute{}.TableName(),
		model.MonthlySkuSegmentPathMapping{}.TableName(),
		model.SellerConfig{}.TableName(),
	)
}

func (ts *enrichCatalogDataTestSuite) setUp() {
	// Init Seller Config
	ts.faker.SellerConfig(&model.SellerConfig{
		SellerId:          1,
		UseDemandPlanning: true,
	}, true)
	ts.faker.SellerConfig(&model.SellerConfig{
		SellerId:          2,
		UseDemandPlanning: false,
	}, true)
	// Init MonthlyCategory
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  1,
		Code:        "CATE1",
		Name:        "Category 1",
		MonthOfYear: "2023-09",
	}, true)
	ts.faker.MonthlyCategory(&model.MonthlyCategory{
		SellerId:    1,
		CategoryId:  2,
		Code:        "CATE2",
		Name:        "Category 2",
		MonthOfYear: "2023-09",
	}, true)
	// Init Monthly Segment
	ts.faker.MonthlySegment(&model.MonthlySegment{
		SellerId:    1,
		CategoryId:  7,
		AttributeId: 3,
		Level:       1,
		MonthOfYear: "2023-09",
	}, true)
	ts.faker.MonthlySegment(&model.MonthlySegment{
		SellerId:    1,
		CategoryId:  7,
		AttributeId: 1,
		Level:       2,
		MonthOfYear: "2023-09",
	}, true)
	ts.faker.MonthlySegment(&model.MonthlySegment{
		SellerId:    1,
		CategoryId:  7,
		AttributeId: 2,
		Level:       3,
		MonthOfYear: "2023-09",
	}, true)
	// Init MonthlySkuCategoryMapping
	ts.faker.MonthlySkuCategoryMapping(&model.MonthlySkuCategoryMapping{
		SellerId:              1,
		Sku:                   "sku10",
		SellerSku:             "sellerSku10",
		Name:                  "sku name 10",
		LatestChildCategoryId: 10,
		MonthOfYear:           "2023-10",
	}, true)
	// Init MonthlySkuSegmentPathMapping
	ts.faker.MonthlySkuSegmentPathMapping(&model.MonthlySkuSegmentPathMapping{
		SellerId:    1,
		Sku:         "sku10",
		SegmentPath: "1/2/3",
		MonthOfYear: "2023-10",
	}, true)
}

func (ts *enrichCatalogDataTestSuite) Test200_EnrichCatalogDataSuccessful() {
	defer ts.tearDown()

	err := ts.job.Run(context.Background())
	assert.Nil(ts.T(), err)

	var monthlyCategories []*model.MonthlyCategory
	_ = ts.db.Table(model.MonthlyCategory{}.TableName()).Find(&monthlyCategories).Error
	for _, data := range monthlyCategories {
		data.Id = 0
		data.CreatedAt = ts.fixedTime
		data.UpdatedAt = ts.fixedTime
	}

	var monthlySegments []*model.MonthlySegment
	_ = ts.db.Table(model.MonthlySegment{}.TableName()).Order("month_of_year, category_id, attribute_id").Find(&monthlySegments).Error
	for _, data := range monthlySegments {
		data.Id = 0
		data.CreatedAt = ts.fixedTime
		data.UpdatedAt = ts.fixedTime
	}

	var monthlySkuCategoryMappings []*model.MonthlySkuCategoryMapping
	_ = ts.db.Table(model.MonthlySkuCategoryMapping{}.TableName()).Find(&monthlySkuCategoryMappings).Error
	for _, data := range monthlySkuCategoryMappings {
		data.Id = 0
		data.CreatedAt = ts.fixedTime
		data.UpdatedAt = ts.fixedTime
	}

	var monthlyVariantAttributes []*model.MonthlyVariantAttribute
	_ = ts.db.Table(model.MonthlyVariantAttribute{}.TableName()).Find(&monthlyVariantAttributes).Error
	for _, data := range monthlyVariantAttributes {
		data.Id = 0
		data.CreatedAt = ts.fixedTime
		data.UpdatedAt = ts.fixedTime
	}

	var monthlySkuSegmentPathMappings []*model.MonthlySkuSegmentPathMapping
	_ = ts.db.Table(model.MonthlySkuSegmentPathMapping{}.TableName()).Find(&monthlySkuSegmentPathMappings).Error
	for _, data := range monthlySkuSegmentPathMappings {
		data.Id = 0
		data.CreatedAt = ts.fixedTime
		data.UpdatedAt = ts.fixedTime
	}

	enrichData := struct {
		MonthlyCategories             []*model.MonthlyCategory
		MonthlySegments               []*model.MonthlySegment
		MonthlySkuCategoryMappings    []*model.MonthlySkuCategoryMapping
		MonthlyVariantAttribute       []*model.MonthlyVariantAttribute
		MonthlySkuSegmentPathMappings []*model.MonthlySkuSegmentPathMapping
	}{
		MonthlyCategories:             monthlyCategories,
		MonthlySegments:               monthlySegments,
		MonthlySkuCategoryMappings:    monthlySkuCategoryMappings,
		MonthlyVariantAttribute:       monthlyVariantAttributes,
		MonthlySkuSegmentPathMappings: monthlySkuSegmentPathMappings,
	}

	goldie.New(ts.T()).AssertJson(ts.T(), "enrich_catalog_data/happy_case_enrich_catalog_data_successful", enrichData)
}
