package consumer

import (
	"context"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	exportServiceApi "go.tekoapis.com/tekone/app/aggregator/export-service/api"
	medusaApi "go.tekoapis.com/tekone/app/supplychain/medusa-service/api"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/api"
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
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/faker"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/tests"
	whCentralApi "go.tekoapis.com/tekone/app/warehouse/central-service/api"
	"go.tekoapis.com/tekone/library/test/monkey"
)

const mockUrl = "www.nice-url.com/this-is-an-url"

type exportSuppliersTestSuite struct {
	tests.TestSuite
	suppliers  []model.Supplier
	worker     *exports.Worker
	faker      *faker.Faker
	flagClient flagsup.ClientAdapter
	flags      map[string]bool
}

func TestExportSuppliers(t *testing.T) {
	ts := &exportSuppliersTestSuite{}

	db, logger = tests.InitIntegrationTestMain(t, db, logger)
	ts.DB = db
	ts.Faker = faker.New(db)
	ts.flags = make(map[string]bool)

	err := ts.setUp()
	if err != nil {
		ts.T().Fatal("Can't setup data to test", err)
	}

	supplyChainClient := &supplychain.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(supplyChainClient), "GetSupplierDelivery",
		func(c *supplychain.Client, ctx context.Context, req *medusaApi.GetSupplierDeliveryRequest) (*medusaApi.GetSupplierDeliveryResponse, error) {
			return &medusaApi.GetSupplierDeliveryResponse{
				Code:    0,
				Message: "Thao tác thành công",
				Suppliers: []*medusaApi.SupplierSiteDeliveryLine{
					{
						SupplierCode: "SUPPLIER1",
						SiteId:       1,
						SiteCode:     "SITE1",
						LeadTime:     20,
						CutOffTime: &types.StringValue{
							Value: "cutoff",
						},
						Mov: &types.DoubleValue{
							Value: 12.2,
						},
						Active: true,
						SoNgayDoiTraTruocKiHan: &types.Int32Value{
							Value: 123,
						},
						SupplierId: 1,
					},
					{
						SupplierCode: "SUPPLIER1",
						SiteId:       2,
						SiteCode:     "SITE2",
						LeadTime:     10,
						Mov: &types.DoubleValue{
							Value: 12.2,
						},
						Active:     false,
						SupplierId: 1,
					},
					{
						SupplierCode: "SUPPLIER2",
						SiteCode:     "SITE1",
						SiteId:       1,
						LeadTime:     20,
						Active:       false,
						SupplierId:   2,
					},
				},
				Total: 3,
			}, nil
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

	fileServiceClient := &file_service.FilesClientImpl{}
	monkey.PatchInstanceMethod(reflect.TypeOf(fileServiceClient), "UploadFile",
		func(c *file_service.FilesClientImpl, filePath string, fileType string, isSetFileName bool) (*types.StringValue, error) {
			return helper.StringToProtoString(mockUrl), nil
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
			return ts.flags[flagsup.FlagOMNI1148]
		})
	monkey.PatchInstanceMethod(reflect.TypeOf(flagClient), "IsEnabled1589ForSeller",
		func(c *flagsup.Client, ctx context.Context, sellerId int32) bool {
			return ts.flags[flagsup.FlagOMNI1589]
		})
	monkey.PatchInstanceMethod(reflect.TypeOf(sellerServiceClient), "GetSellerDetail",
		func(client *seller_service.Client, ctx context.Context, sellerID int32) (seller_service.Seller, error) {
			return seller_service.Seller{CreatedBySeller: 66}, nil
		})

	baseService := &service.Service{
		DB:                  db,
		SupplierRepository:  repository.NewSupplierRepository(db),
		SupplyChainClient:   supplyChainClient,
		WHCentralClient:     whCentralClient,
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

func (ts *exportSuppliersTestSuite) tearDown() {
	err := helper.Truncate(ts.DB,
		model.Supplier{}.TableName(),
	)
	if err != nil {
		ts.T().Fatal("Can't tear down DB", err)
	}
}

func (ts *exportSuppliersTestSuite) setUp() error {
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

	ts.tearDown()
	ts.suppliers = make([]model.Supplier, 3)

	ts.suppliers[2] = *ts.Faker.Supplier(&model.Supplier{
		Id:            1,
		SellerId:      1,
		Code:          "SUPPLIER1",
		Name:          "supplier 1",
		OrderSchedule: orderSchedule,
		InvoiceFormNo: helper.StringToSqlString("INVOICE"),
		InvoiceSign:   helper.StringToSqlString("INVOICE"),
	}, true)

	ts.suppliers[2] = *ts.Faker.Supplier(&model.Supplier{
		Id:            2,
		SellerId:      1,
		Code:          "SUPPLIER2",
		Name:          "supplier 2",
		OrderSchedule: everyDay,
		InvoiceFormNo: helper.StringToSqlString("INVOICE"),
		InvoiceSign:   helper.StringToSqlString("INVOICE"),
	}, true)

	return nil
}

func (ts *exportSuppliersTestSuite) Test_HappyCase() {
	url, err := ts.worker.ExportSuppliers(context.Background(), &exportServiceApi.ExportEvent{
		Id:         1,
		RequestId:  "request-1",
		SellerId:   1,
		ExportType: exportServiceApi.ExportType_sm_export_suppliers.String(),
		Payload:    "{}",
		Status:     exportServiceApi.Status_open.String(),
	})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), mockUrl, url)
}

func (ts *exportSuppliersTestSuite) Test_HappyCaseEpic1148() {
	ts.flags[flagsup.FlagOMNI1148] = true
	url, err := ts.worker.ExportSuppliers(context.Background(), &exportServiceApi.ExportEvent{
		Id:         1,
		RequestId:  "request-1",
		SellerId:   1,
		ExportType: exportServiceApi.ExportType_sm_export_suppliers.String(),
		Payload:    "{}",
		Status:     exportServiceApi.Status_open.String(),
	})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), mockUrl, url)
}

func (ts *exportSuppliersTestSuite) Test_HappyCaseEpic1589() {
	ts.flags[flagsup.FlagOMNI1589] = true
	url, err := ts.worker.ExportSuppliers(context.Background(), &exportServiceApi.ExportEvent{
		Id:         1,
		RequestId:  "request-1",
		SellerId:   1,
		ExportType: exportServiceApi.ExportType_sm_export_suppliers.String(),
		Payload:    "{}",
		Status:     exportServiceApi.Status_open.String(),
	})
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), mockUrl, url)
}
