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

	"github.com/360EntSecGroup-Skylar/excelize/v3"
	"github.com/gogo/protobuf/types"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	exportServiceApi "go.tekoapis.com/tekone/app/aggregator/export-service/api"
	catalogGrpcApi "go.tekoapis.com/tekone/app/catalog/api"
	medusaApi "go.tekoapis.com/tekone/app/supplychain/medusa-service/api"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/api"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/catalog_grpc"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/export_service"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/file_service"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/flagsup"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/seller_service"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/supplychain"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/whcentral"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/consumer/exports"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/provider"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/rpcimpl"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/transformer"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/validator"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/constant"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/faker"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/tests"
	whCentralApi "go.tekoapis.com/tekone/app/warehouse/central-service/api"
	"go.tekoapis.com/tekone/library/test/monkey"
)

var isUpdateExcelFile = flag.Bool("update-excel", false, "is update or not")

type exportSupplierTermsTestSuite struct {
	tests.TestSuite
	suppliers  []model.Supplier
	worker     *exports.Worker
	faker      *faker.Faker
	flagClient flagsup.ClientAdapter

	respGetSupplierDeliveryLayerSupplier        []*medusaApi.SupplierSiteDeliveryLine
	respGetSupplierDeliveryLayerSiteSupplier    []*medusaApi.SupplierSiteDeliveryLine
	respGetSupplierDeliveryLayerSkuSiteSupplier []*medusaApi.SupplierSiteDeliveryLine

	isGenNewFile  bool
	mapResultFile map[string]*excelize.File
	flagList      map[string]bool
}

type sheetDataCompareGoldie struct {
	SheetGeneral         [][]string
	SheetSiteSupplier    [][]string
	SheetSkuSiteSupplier [][]string
}

func TestExportSupplierTerms(t *testing.T) {
	ts := &exportSupplierTermsTestSuite{}

	db, logger = tests.InitIntegrationTestMain(t, db, logger)
	ts.DB = db
	ts.Faker = faker.New(db)
	ts.Context = context.Background()
	ts.mapResultFile = make(map[string]*excelize.File)
	ts.tearDown()
	ts.flagList = make(map[string]bool)

	if isUpdateExcelFile != nil {
		ts.isGenNewFile = *isUpdateExcelFile
	}

	supplyChainClient := &supplychain.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(supplyChainClient), "GetSupplierDeliveryWithBatch",
		func(c *supplychain.Client, ctx context.Context, req *medusaApi.GetSupplierDeliveryRequest) (*medusaApi.GetSupplierDeliveryResponse, error) {
			resp := &medusaApi.GetSupplierDeliveryResponse{
				Suppliers: []*medusaApi.SupplierSiteDeliveryLine{},
			}
			switch req.LayerType.Value {
			case constant.LayerTypeSupplierDeliverySupplier:
				resp.Suppliers = ts.respGetSupplierDeliveryLayerSupplier
			case constant.LayerTypeSupplierDeliverySiteSupplier:
				resp.Suppliers = ts.respGetSupplierDeliveryLayerSiteSupplier
			case constant.LayerTypeSupplierDeliverySkuSiteSupplier:
				resp.Suppliers = ts.respGetSupplierDeliveryLayerSkuSiteSupplier
			}
			return resp, nil
		})

	whCentralClient := &whcentral.ImplClient{}
	monkey.PatchInstanceMethod(reflect.TypeOf(whCentralClient), "GetMapSiteId2SiteInfo",
		func(c *whcentral.ImplClient, ctx context.Context, sellerId int32) (map[int32]whCentralApi.Site, error) {
			return map[int32]whCentralApi.Site{
				1: {
					Id:       1,
					Code:     "SITE1",
					Name:     "site 1",
					SellerId: 1,
				},
				2: {
					Id:       2,
					Code:     "SITE2",
					Name:     "site 2",
					SellerId: 1,
				},
			}, nil
		})

	catalogGrpcClient := &catalog_grpc.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(catalogGrpcClient), "MapSku2ProductInfo",
		func(client *catalog_grpc.Client, ctx context.Context, sellerId int32, skus []string) (map[string]*catalogGrpcApi.BaseProductInfo, error) {
			return map[string]*catalogGrpcApi.BaseProductInfo{
				"sku1": {
					Sku:       "sku1",
					SellerSku: "sellerSku1",
					Name:      "name 1",
				},
				"sku2": {
					Sku:       "sku2",
					SellerSku: "sellerSku2",
					Name:      "name 2",
				},
			}, nil
		})

	fileServiceClient := &file_service.FilesClientImpl{}
	monkey.PatchInstanceMethod(reflect.TypeOf(fileServiceClient), "UploadFile",
		func(c *file_service.FilesClientImpl, filePath string, contentType string, isSetFileName bool) (*types.StringValue, error) {
			fileReader, err := os.ReadFile(filePath)
			assert.Nil(ts.T(), err)
			file, err := excelize.OpenReader(bytes.NewReader(fileReader))
			assert.Nil(ts.T(), err)
			ts.mapResultFile[filePath] = file

			return helper.StringToProtoString(filePath), nil
		})

	exportServiceClient := &export_service.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(exportServiceClient), "GetExportRequest",
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

	monkey.PatchInstanceMethod(reflect.TypeOf(exportServiceClient), "UpdateExportRequest",
		func(c *export_service.Client, ctx context.Context, reqExport *exportServiceApi.UpdateExportRequestRequest) (*exportServiceApi.UpdateExportRequestResponse, error) {
			return &exportServiceApi.UpdateExportRequestResponse{}, nil
		})

	flagClient := &flagsup.Client{}
	ts.flagClient = flagClient
	sellerServiceClient := &seller_service.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(flagClient), "IsEpic1148Enabled",
		func(client *flagsup.Client, ctx context.Context) bool {
			return ts.flagList[flagsup.FlagOMNI1148]
		})
	monkey.PatchInstanceMethod(reflect.TypeOf(flagClient), "IsEnabled1589ForSeller",
		func(p *flagsup.Client, ctx context.Context, sellerId int32) bool {
			return ts.flagList[flagsup.FlagOMNI1589]
		})
	monkey.PatchInstanceMethod(reflect.TypeOf(sellerServiceClient), "GetSellerDetail",
		func(client *seller_service.Client, ctx context.Context, sellerID int32) (seller_service.Seller, error) {
			return seller_service.Seller{CreatedBySeller: 1}, nil
		})

	baseService := &service.Service{
		DB:                  db,
		SupplierRepository:  repository.NewSupplierRepository(db),
		SupplyChainClient:   supplyChainClient,
		WHCentralClient:     whCentralClient,
		CatalogGrpcClient:   catalogGrpcClient,
		FileServiceClient:   fileServiceClient,
		ExportServiceClient: exportServiceClient,
		SupplierProvider:    provider.NewSupplierProvider(sellerServiceClient, flagClient, db),
		FlagSupClient:       flagClient,
	}

	val := validator.NewValidator(db)
	trans := transformer.NewTransformer()

	baseServer := &rpcimpl.Server{
		Service:     baseService,
		Validator:   val,
		Transformer: trans,
	}

	ts.worker = exports.NewWorker(baseService)

	conn, server := ts.InitServerAndClient(t, baseServer)

	suite.Run(t, ts)

	defer func() {
		err := conn.Close()
		if err != nil {
			logger.Error(err, "could not close connection")
		}
		server.Stop()
		ts.tearDown()
	}()
}

func (ts *exportSupplierTermsTestSuite) tearDown() {
	err := helper.Truncate(ts.DB,
		model.Supplier{}.TableName(),
	)
	if err != nil {
		ts.T().Fatal("Can't tear down DB", err)
	}
}

func (ts *exportSupplierTermsTestSuite) setUp() {
	orderSchedule, err := helper.ProtoMessageToJson(&api.OrderSchedule{
		IsOrderableOnMonday:    true,
		IsOrderableOnTuesday:   true,
		IsOrderableOnWednesday: false,
		IsOrderableOnThursday:  false,
		IsOrderableOnFriday:    true,
		IsOrderableOnSaturday:  true,
		IsOrderableOnSunday:    false,
	})
	if err != nil {
		ts.T().Fatal("Can't generate orderSchedule JSON", err)
	}
	everyDay, err := helper.ProtoMessageToJson(&api.OrderSchedule{
		IsOrderableOnMonday:    true,
		IsOrderableOnTuesday:   true,
		IsOrderableOnWednesday: true,
		IsOrderableOnThursday:  true,
		IsOrderableOnFriday:    true,
		IsOrderableOnSaturday:  true,
		IsOrderableOnSunday:    true,
	})
	if err != nil {
		ts.T().Fatal("Can't generate orderSchedule JSON", err)
	}

	ts.Faker.Supplier(&model.Supplier{
		Id:            1,
		SellerId:      1,
		Code:          "SUPPLIER1",
		Name:          "supplier 1",
		OrderSchedule: orderSchedule,
		InvoiceFormNo: helper.StringToSqlString("INVOICE"),
		InvoiceSign:   helper.StringToSqlString("INVOICE"),
	}, true)
	ts.Faker.Supplier(&model.Supplier{
		Id:            2,
		SellerId:      1,
		Code:          "SUPPLIER2",
		Name:          "supplier 2",
		OrderSchedule: everyDay,
		InvoiceFormNo: helper.StringToSqlString("INVOICE"),
		InvoiceSign:   helper.StringToSqlString("INVOICE"),
	}, true)
}

func (ts *exportSupplierTermsTestSuite) assert(req exportServiceApi.PayloadSCExportSupplierTerms, expectFileName string) {
	defer func() {
		ts.tearDown()
	}()

	payload, err := json.Marshal(req)
	if err != nil {
		ts.T().Fatal("error marshal req")
	}

	actualUrl, err := ts.worker.ExportSupplierTerms(ts.Context, &exportServiceApi.ExportEvent{
		Id:         1,
		RequestId:  "request-1",
		SellerId:   1,
		ExportType: exportServiceApi.ExportType_sm_export_suppliers.String(),
		Payload:    string(payload),
		Status:     exportServiceApi.Status_open.String(),
	})
	assert.Nil(ts.T(), err)
	expectedPath := fmt.Sprintf("test_data/export_supplier_terms/%s", expectFileName)
	if ts.isGenNewFile {
		_ = ts.mapResultFile[actualUrl].SaveAs(fmt.Sprintf("%s.xlsx", expectedPath))
	}
	fileReader, err := os.ReadFile(fmt.Sprintf("%s.xlsx", expectedPath))
	assert.Nil(ts.T(), err)
	expected, err := excelize.OpenReader(bytes.NewReader(fileReader))
	assert.Nil(ts.T(), err)
	actual := ts.mapResultFile[actualUrl]
	actualData := sheetDataCompareGoldie{}

	// assert sheet general
	if req.IsExportSupplierTerms {
		cols, err := actual.GetRows(exports.ExportSupplierTermsSheetGeneral)
		assert.Nil(ts.T(), err)
		actualData.SheetGeneral = cols

		maxRow := 30
		maxColSheetGeneral := 13
		for row := 1; row < maxRow; row++ {
			for col := 1; col < maxColSheetGeneral; col++ {
				colName, _ := excelize.ColumnNumberToName(col)
				rs1, err := helper.GetCell(colName, row, expected, exports.ExportSupplierTermsSheetGeneral)
				assert.Nil(ts.T(), err)
				rs2, err := helper.GetCell(colName, row, actual, exports.ExportSupplierTermsSheetGeneral)
				assert.Nil(ts.T(), err)
				assert.Equal(ts.T(), rs1, rs2, fmt.Sprintf("Row %d Col %d", row, col))
			}
		}
	}

	// assert sheet site supplier and sku site supplier
	if req.IsExportSiteSkuSupplierTerms {
		// assert sheet site supplier
		cols, err := actual.GetRows(exports.ExportSupplierTermsSheetSiteSupplier)
		assert.Nil(ts.T(), err)
		actualData.SheetSiteSupplier = cols

		maxRow := 30
		maxColSheetSiteSupplier := 8
		for row := 1; row < maxRow; row++ {
			for col := 1; col < maxColSheetSiteSupplier; col++ {
				colName, _ := excelize.ColumnNumberToName(col)
				rs1, err := helper.GetCell(colName, row, expected, exports.ExportSupplierTermsSheetSiteSupplier)
				assert.Nil(ts.T(), err)
				rs2, err := helper.GetCell(colName, row, actual, exports.ExportSupplierTermsSheetSiteSupplier)
				assert.Nil(ts.T(), err)
				assert.Equal(ts.T(), rs1, rs2, fmt.Sprintf("Row %d Col %d", row, col))
			}
		}

		// assert sheet sku site supplier
		cols, err = actual.GetRows(exports.ExportSupplierTermsSheetSkuSiteSupplier)
		assert.Nil(ts.T(), err)
		actualData.SheetSkuSiteSupplier = cols

		maxColSheetSkuSiteSupplier := 9
		for row := 1; row < maxRow; row++ {
			for col := 1; col < maxColSheetSkuSiteSupplier; col++ {
				colName, _ := excelize.ColumnNumberToName(col)
				rs1, err := helper.GetCell(colName, row, expected, exports.ExportSupplierTermsSheetSkuSiteSupplier)
				assert.Nil(ts.T(), err)
				rs2, err := helper.GetCell(colName, row, actual, exports.ExportSupplierTermsSheetSkuSiteSupplier)
				assert.Nil(ts.T(), err)
				assert.Equal(ts.T(), rs1, rs2, fmt.Sprintf("Row %d Col %d", row, col))
			}
		}
	}

	goldie.New(ts.T()).AssertJson(ts.T(), fmt.Sprintf("export_supplier_terms/%s", expectFileName), actualData)
}

func (ts *exportSupplierTermsTestSuite) Test200_ExportSheetThongTinChung_WithoutFilter() {
	ts.setUp()
	ts.assert(exportServiceApi.PayloadSCExportSupplierTerms{
		SellerId:              1,
		IsExportSupplierTerms: true,
	}, "200_export_sheet_thong_tin_chung_without_filter")
}

func (ts *exportSupplierTermsTestSuite) Test200_ExportSheetThongTinChung_WithFilter() {
	ts.setUp()
	ts.assert(exportServiceApi.PayloadSCExportSupplierTerms{
		SellerId:              1,
		IsExportSupplierTerms: true,
		SupplierIds:           []int32{1},
	}, "200_export_sheet_thong_tin_chung_with_filter")
}

func (ts *exportSupplierTermsTestSuite) Test200_ExportSheetSupplierSite_WithoutFilter() {
	ts.setUp()
	ts.respGetSupplierDeliveryLayerSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       0,
			LeadTime:     10,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 120000,
			},
			Moq: &types.DoubleValue{
				Value: 10,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       0,
			LeadTime:     9,
			Mov: &types.DoubleValue{
				Value: 150000,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 11000,
			},
			Moq: &types.DoubleValue{
				Value: 5,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       2,
			SiteCode:     "SITE2",
			LeadTime:     7,
			Mov: &types.DoubleValue{
				Value: 50000,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSkuSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			Sku:          helper.StringToProtoString("sku1"),
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 5000,
			},
			Moq: &types.DoubleValue{
				Value: 2,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       2,
			SiteCode:     "SITE2",
			Sku:          helper.StringToProtoString("sku2"),
			LeadTime:     8,
			Mov: &types.DoubleValue{
				Value: 13000,
			},
			Moq: &types.DoubleValue{
				Value: 3,
			},
		},
	}
	ts.assert(exportServiceApi.PayloadSCExportSupplierTerms{
		SellerId:                     1,
		IsExportSiteSkuSupplierTerms: true,
	}, "200_export_sheet_supplier_site_without_filter")
}

func (ts *exportSupplierTermsTestSuite) Test200_ExportSheetSupplierSite_WithFilter() {
	ts.setUp()
	ts.respGetSupplierDeliveryLayerSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       0,
			LeadTime:     10,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 120000,
			},
			Moq: &types.DoubleValue{
				Value: 10,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 11000,
			},
			Moq: &types.DoubleValue{
				Value: 5,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSkuSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			Sku:          helper.StringToProtoString("sku1"),
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 5000,
			},
			Moq: &types.DoubleValue{
				Value: 2,
			},
		},
	}
	ts.assert(exportServiceApi.PayloadSCExportSupplierTerms{
		SellerId:                     1,
		IsExportSiteSkuSupplierTerms: true,
		SupplierIds:                  []int32{1},
	}, "200_export_sheet_supplier_site_with_filter")
}

func (ts *exportSupplierTermsTestSuite) Test200_ExportAllSheet() {
	ts.setUp()
	ts.respGetSupplierDeliveryLayerSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       0,
			LeadTime:     10,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 120000,
			},
			Moq: &types.DoubleValue{
				Value: 10,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       0,
			LeadTime:     9,
			Mov: &types.DoubleValue{
				Value: 150000,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 11000,
			},
			Moq: &types.DoubleValue{
				Value: 5,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       2,
			SiteCode:     "SITE2",
			LeadTime:     7,
			Mov: &types.DoubleValue{
				Value: 50000,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSkuSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			Sku:          helper.StringToProtoString("sku1"),
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 5000,
			},
			Moq: &types.DoubleValue{
				Value: 2,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       2,
			SiteCode:     "SITE2",
			Sku:          helper.StringToProtoString("sku2"),
			LeadTime:     8,
			Mov: &types.DoubleValue{
				Value: 13000,
			},
			Moq: &types.DoubleValue{
				Value: 3,
			},
		},
	}
	ts.assert(exportServiceApi.PayloadSCExportSupplierTerms{
		SellerId:                     1,
		IsExportSiteSkuSupplierTerms: true,
		IsExportSupplierTerms:        true,
	}, "200_export_all_sheet")
}

func (ts *exportSupplierTermsTestSuite) Test200_ExportAllSheet_Omni1589() {
	ts.setUp()
	ts.flagList[flagsup.FlagOMNI1589] = true
	defer func() {
		ts.flagList = make(map[string]bool)
	}()
	ts.respGetSupplierDeliveryLayerSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       0,
			LeadTime:     10,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 120000,
			},
			Moq: &types.DoubleValue{
				Value: 10,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       0,
			LeadTime:     9,
			Mov: &types.DoubleValue{
				Value: 150000,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 11000,
			},
			Moq: &types.DoubleValue{
				Value: 5,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       2,
			SiteCode:     "SITE2",
			LeadTime:     7,
			Mov: &types.DoubleValue{
				Value: 50000,
			},
		},
	}
	ts.respGetSupplierDeliveryLayerSkuSiteSupplier = []*medusaApi.SupplierSiteDeliveryLine{
		{
			SupplierId:   1,
			SupplierCode: "SUPPLIER1",
			SiteId:       1,
			SiteCode:     "SITE1",
			Sku:          helper.StringToProtoString("sku1"),
			LeadTime:     20,
			CutOffTime: &types.StringValue{
				Value: "cutoff",
			},
			Mov: &types.DoubleValue{
				Value: 5000,
			},
			Moq: &types.DoubleValue{
				Value: 2,
			},
		},
		{
			SupplierId:   2,
			SupplierCode: "SUPPLIER2",
			SiteId:       2,
			SiteCode:     "SITE2",
			Sku:          helper.StringToProtoString("sku2"),
			LeadTime:     8,
			Mov: &types.DoubleValue{
				Value: 13000,
			},
			Moq: &types.DoubleValue{
				Value: 3,
			},
		},
	}
	ts.assert(exportServiceApi.PayloadSCExportSupplierTerms{
		SellerId:                     1,
		IsExportSiteSkuSupplierTerms: true,
		IsExportSupplierTerms:        true,
	}, "200_export_all_sheet_omni_1589")
}
