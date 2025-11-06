package contact_management

import (
	"context"
	"reflect"
	"testing"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/datatypes"

	"go.tekoapis.com/tekone/app/supplychain/supplier_service/api"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/config"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/flagsup"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/seller_service"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/srm"
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
	"go.tekoapis.com/tekone/library/test/monkey"
)

type getSuppliersTestSuite struct {
	tests.TestSuite
	suppliers  []model.Supplier
	flagClient flagsup.ClientAdapter
}

func (ts *getSuppliersTestSuite) tearDown() {
	err := helper.Truncate(ts.DB,
		model.Supplier{}.TableName(),
		model.SupplierContact{}.TableName(),
		model.SupplierContactCategory{}.TableName(),
	)
	if err != nil {
		ts.T().Fatal("Can't tear down DB", err)
	}
}

func (ts *getSuppliersTestSuite) setUp() error {
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

	ts.tearDown()
	ts.suppliers = make([]model.Supplier, 6)
	ts.suppliers[0] = ts.Faker.SupplierInfo(1, 1, true)
	ts.suppliers[1] = ts.Faker.SupplierInfo(1, 2, true)

	ts.suppliers[2] = *ts.Faker.Supplier(&model.Supplier{
		Id:            3,
		SellerId:      2,
		Code:          "sup3",
		Name:          "supplier 3",
		OrderSchedule: orderSchedule,
		InvoiceFormNo: helper.StringToSqlString("INVOICE"),
		InvoiceSign:   helper.StringToSqlString("INVOICE"),
	}, true)

	ts.suppliers[3] = *ts.Faker.Supplier(&model.Supplier{
		Id:       4,
		SellerId: 3,
		Code:     "sup4",
		Name:     "supplier 4",
		IsActive: helper.BoolToSqlBool(true),
	}, true)

	ts.suppliers[4] = *ts.Faker.Supplier(&model.Supplier{
		Id:       5,
		SellerId: 3,
		Code:     "sup5",
		Name:     "supplier 5",
		IsActive: helper.BoolToSqlBool(false),
	}, true)

	ts.suppliers[5] = *ts.Faker.Supplier(&model.Supplier{
		Id:       6,
		SellerId: 66,
		Code:     "sup6",
		Name:     "supplier 6",
		IsActive: helper.BoolToSqlBool(true),
	}, true)

	return nil
}

func TestGetSuppliers(t *testing.T) {
	ts := &getSuppliersTestSuite{}

	db, logger = tests.InitIntegrationTestMain(t, db, logger)
	ts.DB = db
	ts.Faker = faker.New(db)

	err := ts.setUp()
	if err != nil {
		ts.T().Fatal("Can't setup data to test", err)
	}
	srmClient := &srm.Client{}
	flagClient := &flagsup.Client{}
	ts.flagClient = flagClient
	sellerServiceClient := &seller_service.Client{}

	monkey.PatchInstanceMethod(reflect.TypeOf(srmClient), "ListPartners",
		func(client *srm.Client, ctx context.Context, req srm.ListPartnersRequest) ([]srm.Partner, error) {
			return []srm.Partner{
				{
					Id:   100,
					Code: "supplier100",
					Name: "supplier100",
				},
			}, nil
		})
	monkey.PatchInstanceMethod(reflect.TypeOf(flagClient), "IsEpic1148Enabled",
		func(client *flagsup.Client, ctx context.Context) bool {
			return false
		})
	monkey.PatchInstanceMethod(reflect.TypeOf(sellerServiceClient), "GetSellerDetail",
		func(client *seller_service.Client, ctx context.Context, sellerID int32) (seller_service.Seller, error) {
			return seller_service.Seller{CreatedBySeller: 66}, nil
		})

	baseService := &service.Service{
		Config: &config.Config{
			SellersOn811: "1,3",
		},
		DB:                 db,
		SupplierRepository: repository.NewSupplierRepository(db),
		SrmClient:          srmClient,
		SupplierProvider:   provider.NewSupplierProvider(sellerServiceClient, flagClient, db),
	}

	val := validator.NewValidator(db)
	trans := transformer.NewTransformer()

	baseServer := &rpcimpl.Server{
		Service:     baseService,
		Validator:   val,
		Transformer: trans,
	}

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

func (ts *getSuppliersTestSuite) TestInvalidParam() {
	ts.Run("not pass sellerId", func() {
		req := &api.GetSuppliersRequest{}
		ts.assertError(req, "rpc error: code = InvalidArgument desc = invalid GetSuppliersRequest.SellerId: value must be greater than or equal to 1")
	})

	ts.Run("has invalid invalid int32 - string in supplierIds", func() {
		req := &api.GetSuppliersRequest{
			SellerId:    1,
			SupplierIds: "1,a",
		}
		ts.assertError(req, "rpc error: code = InvalidArgument desc = SupplierIds phải là các số nguyên cách nhau bởi dấu phẩy")
	})

	ts.Run("has invalid invalid int32 - float in supplierIds", func() {
		req := &api.GetSuppliersRequest{
			SellerId:    1,
			SupplierIds: "1, 1.5",
		}
		ts.assertError(req, "rpc error: code = InvalidArgument desc = SupplierIds phải là các số nguyên cách nhau bởi dấu phẩy")
	})

	ts.Run("has invalid status", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 1,
			Statuses: []string{"invalidStatus"},
		}
		ts.assertError(req, "rpc error: code = InvalidArgument desc = invalid GetSuppliersRequest.Statuses[0]: value must be in list [active inactive]")
	})

	ts.Run("duplicate status", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 1,
			Statuses: []string{"active", "active"},
		}
		ts.assertError(req, "rpc error: code = InvalidArgument desc = Status bị trùng lặp")
	})
}

func (ts *getSuppliersTestSuite) TestHappyCase() {
	ts.Run("not filter by supplierIds - get all", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 1,
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 2, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[0], suppliers[0])
		ts.assertSupplierInfo(ts.suppliers[1], suppliers[1])
		goldie.New(ts.T()).AssertJson(ts.T(), "happy_case", resp)
	})

	ts.Run("filter by 1 supplierId", func() {
		req := &api.GetSuppliersRequest{
			SellerId:    1,
			SupplierIds: "1",
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 1, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[0], suppliers[0])
		goldie.New(ts.T()).AssertJson(ts.T(), "filter_by_1_supplier_id", resp)
	})

	ts.Run("filter by >= 2 supplierIds", func() {
		req := &api.GetSuppliersRequest{
			SellerId:    1,
			SupplierIds: "1,2",
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 2, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[0], suppliers[0])
		ts.assertSupplierInfo(ts.suppliers[1], suppliers[1])
		goldie.New(ts.T()).AssertJson(ts.T(), "filter_by_more_than_1_supplier_ids", resp)
	})

	ts.Run("filter with supplier has order schedule", func() {
		req := &api.GetSuppliersRequest{
			SellerId:    2,
			SupplierIds: "3",
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 1, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[2], suppliers[0])
		goldie.New(ts.T()).AssertJson(ts.T(), "filter_with_supplier_has_order_schedule", resp)
	})

	ts.Run("filter with status active", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 3,
			Statuses: []string{"active"},
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 1, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[3], suppliers[0])
		goldie.New(ts.T()).AssertJson(ts.T(), "filter_with_status_active", resp)
	})

	ts.Run("filter with status inactive", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 3,
			Statuses: []string{"inactive"},
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 1, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[4], suppliers[0])
		goldie.New(ts.T()).AssertJson(ts.T(), "filter_with_status_inactive", resp)
	})

	ts.Run("filter with all status", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 3,
			Statuses: []string{"active", "inactive"},
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 2, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[3], suppliers[0])
		ts.assertSupplierInfo(ts.suppliers[4], suppliers[1])
		goldie.New(ts.T()).AssertJson(ts.T(), "filter_with_all_status", resp)
	})

	ts.Run("filter with not pass status and supplierIds=> get only inactive", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 3,
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 1, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[3], suppliers[0])
		goldie.New(ts.T()).AssertJson(ts.T(), "get_only_inactive", resp)
	})

	ts.Run("filter with supplierIds => status not effect", func() {
		req := &api.GetSuppliersRequest{
			SellerId:    3,
			SupplierIds: "4",
			Statuses:    []string{"inactive"},
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 1, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[3], suppliers[0])
		goldie.New(ts.T()).AssertJson(ts.T(), "status_not_effect", resp)
	})
}

func (ts *getSuppliersTestSuite) TestHappyCaseEpic1148() {
	monkey.PatchInstanceMethod(reflect.TypeOf(ts.flagClient), "IsEpic1148Enabled",
		func(client *flagsup.Client, ctx context.Context) bool {
			return true
		})

	ts.Run("not filter by supplierIds - get all", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 1,
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 3, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[0], suppliers[0])
		ts.assertSupplierInfo(ts.suppliers[1], suppliers[1])
		ts.assertSupplierInfo(ts.suppliers[5], suppliers[2])
		goldie.New(ts.T()).AssertJson(ts.T(), "get_all_epic_1148", resp)
	})
}

func (ts *getSuppliersTestSuite) TestHappyCaseWithPagination() {
	ts.Run("get all - with pagination", func() {
		req := &api.GetSuppliersRequest{
			SellerId: 1,
			Page:     helper.Int32ToProtoInt32(2),
			PageSize: helper.Int32ToProtoInt32(1),
		}
		resp, suppliers := ts.assertSuccess(req)
		assert.Equal(ts.T(), 1, len(suppliers))
		ts.assertSupplierInfo(ts.suppliers[1], suppliers[0])
		goldie.New(ts.T()).AssertJson(ts.T(), "happy_case_with_pagination", resp)
	})
}

func (ts *getSuppliersTestSuite) assertError(req *api.GetSuppliersRequest, errMessage string) {
	res, err := ts.Client.GetSuppliers(context.Background(), req)
	assert.Nil(ts.T(), res)
	assert.Error(ts.T(), err)
	assert.Equal(ts.T(), errMessage, err.Error())
}

func (ts *getSuppliersTestSuite) assertSuccess(req *api.GetSuppliersRequest) (*api.GetSuppliersResponse, []*api.SupplierInfo) {
	resp, err := ts.Client.GetSuppliers(context.Background(), req)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), constant.CodeSuccess, resp.Code)
	assert.Equal(ts.T(), constant.MessageSuccess, resp.Message)
	return resp, resp.Data.Suppliers
}

func (ts *getSuppliersTestSuite) assertSupplierInfo(expect model.Supplier, resp *api.SupplierInfo) {
	assert.Equal(ts.T(), expect.Id, resp.Id)
	assert.Equal(ts.T(), expect.Code, resp.Code)
	assert.Equal(ts.T(), expect.Name, resp.Name)
	assert.Equal(ts.T(), expect.FullAddress, resp.FullAddress)
	assert.Equal(ts.T(), len(expect.Contacts), len(expect.Contacts))
	assert.Equal(ts.T(), expect.IsActive.Bool, resp.IsActive)
	for i, expectContact := range expect.Contacts {
		respContact := resp.Contacts[i]
		assert.Equal(ts.T(), helper.SqlStringToProtoString(expectContact.Email), respContact.Email)
		assert.Equal(ts.T(), helper.SqlStringToProtoString(expectContact.PhoneNumber), respContact.PhoneNumber)
		assert.Equal(ts.T(), len(expectContact.Categories), len(respContact.CategoryIds))
		if len(expectContact.Categories) == 0 {
			continue
		}
		expectCategoryIds := make([]int32, len(expectContact.Categories))
		for j, catId := range expectContact.Categories {
			expectCategoryIds[j] = catId.CategoryId
		}
		assert.Equal(ts.T(), expectCategoryIds, respContact.CategoryIds)
	}
	if expect.OrderSchedule != nil {
		orderScheduleJson, err := helper.ProtoMessageToJson(resp.OrderSchedule)
		if err != nil {
			ts.T().Fatal("Can't marshall proto to json", err)
		}
		assert.Equal(ts.T(), expect.OrderSchedule, datatypes.JSON(orderScheduleJson))
	}
}
