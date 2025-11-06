package contact_management

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"go.tekoapis.com/tekone/app/supplychain/supplier_service/api"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/audit_log"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/flagsup"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/adapter/identity"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/provider"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/repository"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/rpcimpl"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/transformer"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/internal/validator"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/constant"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/errorz"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/faker"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/tests"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type UpsertSupplierInfoTestSuite struct {
	tests.TestSuite
	Suppliers []model.Supplier
}

func (ts *UpsertSupplierInfoTestSuite) tearDown() {
	err := helper.Truncate(ts.DB,
		model.Supplier{}.TableName(),
		model.SupplierContact{}.TableName(),
		model.SupplierContactCategory{}.TableName(),
	)
	if err != nil {
		ts.T().Fatal("Can't tear down DB", err)
	}
}

func (ts *UpsertSupplierInfoTestSuite) setUp() error {
	ts.tearDown()
	ts.Suppliers = make([]model.Supplier, 1)
	ts.Suppliers[0] = *ts.Faker.Supplier(ts.generateSupplier(100), true)
	return nil
}

func TestUpsertSupplierInfo(t *testing.T) {
	ts := &UpsertSupplierInfoTestSuite{}

	db, logger = tests.InitIntegrationTestMain(t, db, logger)
	ts.DB = db
	ts.Faker = faker.New(db)
	ts.Context = context.Background()

	err := ts.setUp()
	if err != nil {
		ts.T().Fatal("Can't setup data to test", err)
	}

	flagSupClient := &flagsup.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(flagSupClient), "IsEnabled",
		func(client *flagsup.Client, ctx context.Context, flagKey string, fallback bool) bool {
			return false
		})

	identityClient := &identity.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(identityClient), "GetUserBySub",
		func(client *identity.Client, ctx context.Context, sub string) (identity.User, error) {
			return identity.User{
				Id:    "MJM56",
				Email: "mjm.56@mail.com",
				Name:  "MJM 56",
			}, nil
		})

	auditlogClient := &auditlog.Client{}
	monkey.PatchInstanceMethod(reflect.TypeOf(auditlogClient), "SendLog",
		func(client *auditlog.Client, log auditlog.AuditLog) error {
			return nil
		})

	monkey.Patch(helper.NewBackgroundContext, func(ctx context.Context) context.Context {
		return ctx
	})

	baseService := &service.Service{
		DB:                                db,
		RawRepository:                     repository.NewRawRepository(db),
		SupplierRepository:                repository.NewSupplierRepository(db),
		SupplierContactRepository:         repository.NewSupplierContactRepository(db),
		SupplierContactCategoryRepository: repository.NewSupplierContactCategoryRepository(db),
		SupplierProvider:                  provider.NewSupplierProvider(nil, flagSupClient, db),
		FlagSupClient:                     flagSupClient,
		IdentityClient:                    identityClient,
		AuditLogClient:                    auditlogClient,
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
		monkey.UnpatchAll()
	}()
}

func (ts *UpsertSupplierInfoTestSuite) assertSuccess(req *api.UpsertSupplierInfoRequest, expectedData *api.SupplierInfo) {
	res, err := ts.Client.UpsertSupplierInfo(ts.Context, req)

	assert.Nil(ts.T(), err)
	assert.NotNil(ts.T(), res)
	assert.NotNil(ts.T(), res.Data)

	actualData := res.Data
	assert.Equal(ts.T(), expectedData.Name, actualData.Name)
	assert.Equal(ts.T(), expectedData.TaxCode, actualData.TaxCode)
	assert.Equal(ts.T(), expectedData.Address, actualData.Address)
	assert.Equal(ts.T(), expectedData.WardCode, actualData.WardCode)
	assert.Equal(ts.T(), expectedData.WardName, actualData.WardName)
	assert.Equal(ts.T(), expectedData.DistrictCode, actualData.DistrictCode)
	assert.Equal(ts.T(), expectedData.DistrictName, actualData.DistrictName)
	assert.Equal(ts.T(), expectedData.ProvinceCode, actualData.ProvinceCode)
	assert.Equal(ts.T(), expectedData.ProvinceName, actualData.ProvinceName)
	assert.Equal(ts.T(), expectedData.FullAddress, actualData.FullAddress)
	assert.Equal(ts.T(), expectedData.Contacts, actualData.Contacts)
	assert.Equal(ts.T(), expectedData.IsActive, actualData.IsActive)

	if req.Id != 0 {
		assert.Equal(ts.T(), expectedData.Id, actualData.Id)
	}
	if expectedData.Code != constant.EmptyString {
		assert.Equal(ts.T(), expectedData.Code, actualData.Code)
	}
	if req.PaymentAfterInvoiceDays != nil {
		assert.Equal(ts.T(), expectedData.PaymentAfterInvoiceDays, actualData.PaymentAfterInvoiceDays)
	} else {
		assert.Nil(ts.T(), actualData.PaymentAfterInvoiceDays)
	}
	if req.ExchangeableStartingDays != nil {
		assert.Equal(ts.T(), expectedData.ExchangeableStartingDays, actualData.ExchangeableStartingDays)
	} else {
		assert.Nil(ts.T(), actualData.ExchangeableStartingDays)
	}
	if req.OrderSchedule != nil {
		assert.Equal(ts.T(), expectedData.OrderSchedule, actualData.OrderSchedule)
	}
	if req.InvoiceFormNo != nil {
		assert.Equal(ts.T(), expectedData.InvoiceFormNo, actualData.InvoiceFormNo)
	}
	if req.InvoiceSign != nil {
		assert.Equal(ts.T(), expectedData.InvoiceSign, actualData.InvoiceSign)
	}
}

func (ts *UpsertSupplierInfoTestSuite) assertError(req *api.UpsertSupplierInfoRequest, errMessage string) {
	res, err := ts.Client.UpsertSupplierInfo(context.Background(), req)
	assert.Nil(ts.T(), res)
	assert.Error(ts.T(), err)
	assert.Equal(ts.T(), errMessage, err.Error())
}

func (ts *UpsertSupplierInfoTestSuite) generateSupplier(id int32) *model.Supplier {
	return &model.Supplier{
		Id:           id,
		Code:         fmt.Sprintf("s%d", id),
		Name:         fmt.Sprintf("supplier %d", id),
		TaxCode:      fmt.Sprintf("tax%d", id),
		FullAddress:  "47 Phạm Văn Đồng, Phường Cổ Nhuế 1, Quận Bắc Từ Liêm, Thành phố Hà Nội",
		Address:      "47 Phạm Văn Đồng",
		WardCode:     "w1",
		WardName:     "Phường Cổ Nhuế 1",
		DistrictCode: "d2",
		DistrictName: "Quận Bắc Từ Liêm",
		ProvinceCode: "p1",
		ProvinceName: "Thành phố Hà Nội",
		Contacts: []model.SupplierContact{
			{
				Categories: []model.SupplierContactCategory{
					{
						CategoryId: 1,
					},
					{
						CategoryId: 2,
					},
				},
				Email:       helper.StringToSqlString(fmt.Sprintf("supplier%d@sup%d.com", id, id)),
				PhoneNumber: helper.StringToSqlString("113"),
			},
		},
		SellerId: 1,
	}
}

func (ts *UpsertSupplierInfoTestSuite) populateReq(req *api.UpsertSupplierInfoRequest) *api.UpsertSupplierInfoRequest {
	req.FullAddress = "47 Phạm Văn Đồng, Phường Cổ Nhuế 1, Quận Bắc Từ Liêm, Thành phố Hà Nội"
	req.Address = "47 Phạm Văn Đồng"
	req.WardCode = "w1"
	req.WardName = "Phường Cổ Nhuế 1"
	req.DistrictCode = "d2"
	req.DistrictName = "Quận Bắc Từ Liêm"
	req.ProvinceCode = "p1"
	req.ProvinceName = "Thành phố Hà Nội"
	return req
}

func (ts *UpsertSupplierInfoTestSuite) populateResp(resp *api.SupplierInfo) *api.SupplierInfo {
	resp.FullAddress = "47 Phạm Văn Đồng, Phường Cổ Nhuế 1, Quận Bắc Từ Liêm, Thành phố Hà Nội"
	resp.Address = "47 Phạm Văn Đồng"
	resp.WardCode = "w1"
	resp.WardName = "Phường Cổ Nhuế 1"
	resp.DistrictCode = "d2"
	resp.DistrictName = "Quận Bắc Từ Liêm"
	resp.ProvinceCode = "p1"
	resp.ProvinceName = "Thành phố Hà Nội"
	return resp
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_CreateWithCode() {
	ts.Run("Happy case, id = 0, with code", func() {
		supplierId := 1
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Code:                     fmt.Sprintf("s%d", supplierId),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
			IsActive:      true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_CreateWithoutCode() {
	ts.Run("Happy case, id = 0, code not passed", func() {
		supplierId := 2
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_UpdateWithCode() {
	ts.Run("Happy case, id > 0, passed code", func() {
		supplierId := 100
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Id:                       ts.Suppliers[0].Id,
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     helper.StringToProtoString("s2000"),
			TaxCode:                  "tax101",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2, 3},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Id:                       ts.Suppliers[0].Id,
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     fmt.Sprintf("s%d", supplierId),
			TaxCode:                  "tax101",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2, 3},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
			IsActive:      true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_UpdateWithoutCode() {
	ts.Run("Happy case, id > 0, code not passed", func() {
		supplierId := 100
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Id:                       ts.Suppliers[0].Id,
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  "tax102",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Id:                       ts.Suppliers[0].Id,
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     fmt.Sprintf("s%d", supplierId),
			TaxCode:                  "tax102",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
			IsActive:      true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_UpsertWithoutContacts() {
	ts.Run("Happy case, contacts not passed", func() {
		supplierId := 3
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			SellerId:                 1,
			RequestedById:            "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			IsActive:                 true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_UpsertWithoutPaymentAfterInvoiceDays() {
	ts.Run("Happy case, payment after invoice days not passed", func() {
		supplierId := 4
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(2),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(2),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
			IsActive:      true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_UpsertWithoutExchangeableStartingDays() {
	ts.Run("Happy case, exchangeable starting days not passed", func() {
		supplierId := 5
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Name:                    fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                 fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays: helper.Int32ToProtoInt32(2),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                    fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                 fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays: helper.Int32ToProtoInt32(2),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_AllFieldPassed() {
	ts.Run("Happy case, all field passed", func() {
		supplierId := 6
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Code:                     fmt.Sprintf("s%d", supplierId),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
			IsActive:      true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_CategoryAll() {
	ts.Run("Happy case, category all", func() {
		supplierId := 7
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			InvoiceFormNo: helper.StringToProtoString("INVOICE"),
			InvoiceSign:   helper.StringToProtoString("INVOICE"),
			IsActive:      true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_ContactNoEmail() {
	ts.Run("Happy case, contact no email", func() {
		supplierId := 8
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_ContactNoPhone() {
	ts.Run("Happy case, contact no phone", func() {
		supplierId := 9
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
				},
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_ErrorCase_DuplicateCodeWithinSeller() {
	ts.Run("Error case, duplicate code within seller", func() {
		supplierId := 10
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString("s100"),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		ts.assertError(req, errorz.InvalidArgumentError(errorz.SupplierWithSameSellerIdAndCodeAlreadyExist).Error())
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_DuplicateCodeDifferentSeller() {
	ts.Run("Happy case, duplicate code different seller", func() {
		supplierId := 11
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString("s100"),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      2,
			RequestedById: "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     "s100",
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_ErrorCase_MissingContactInfo() {
	ts.Run("Error case, missing contact info", func() {
		supplierId := 12
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		ts.assertError(req, errorz.InvalidArgumentError(errorz.SupplierMissingContactInfo).Error())
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_ErrorCase_DuplicateContactInfo() {
	ts.Run("Error case, missing contact info", func() {
		supplierId := 13
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
				{
					CategoryIds: []int32{3, 4},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		ts.assertError(req, errorz.InvalidArgumentError(errorz.DuplicateSupplierContactInfo).Error())
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_DuplicateTaxCodeWithinSeller() {
	ts.Run("Happy case, duplicate tax code within seller", func() {
		supplierId := 14
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  "tax100",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     fmt.Sprintf("s%d", supplierId),
			TaxCode:                  "tax100",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_DuplicateTaxCodeDifferentSeller() {
	ts.Run("Happy case, duplicate code different seller", func() {
		supplierId := 15
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  "tax100",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      2,
			RequestedById: "MJM56",
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     fmt.Sprintf("s%d", supplierId),
			TaxCode:                  "tax100",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_CreateWithFullOrderSchedule() {
	ts.Run("Happy case, id = 0, with full order schedule", func() {
		supplierId := 16
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			OrderSchedule: &api.OrderSchedule{
				IsOrderableOnMonday:    true,
				IsOrderableOnTuesday:   false,
				IsOrderableOnWednesday: true,
				IsOrderableOnThursday:  false,
				IsOrderableOnFriday:    true,
				IsOrderableOnSaturday:  false,
				IsOrderableOnSunday:    true,
			},
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Code:                     fmt.Sprintf("s%d", supplierId),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			OrderSchedule: &api.OrderSchedule{
				IsOrderableOnMonday:    true,
				IsOrderableOnTuesday:   false,
				IsOrderableOnWednesday: true,
				IsOrderableOnThursday:  false,
				IsOrderableOnFriday:    true,
				IsOrderableOnSaturday:  false,
				IsOrderableOnSunday:    true,
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_UpdateWithFullOrderSchedule() {
	ts.Run("Happy case, id > 0, with full order schedule", func() {
		supplierId := 100
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Id:                       ts.Suppliers[0].Id,
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  "tax101",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2, 3},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			OrderSchedule: &api.OrderSchedule{
				IsOrderableOnMonday:    true,
				IsOrderableOnTuesday:   false,
				IsOrderableOnWednesday: true,
				IsOrderableOnThursday:  false,
				IsOrderableOnFriday:    true,
				IsOrderableOnSaturday:  false,
				IsOrderableOnSunday:    true,
			},
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Id:                       ts.Suppliers[0].Id,
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     fmt.Sprintf("s%d", supplierId),
			TaxCode:                  "tax101",
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2, 3},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			OrderSchedule: &api.OrderSchedule{
				IsOrderableOnMonday:    true,
				IsOrderableOnTuesday:   false,
				IsOrderableOnWednesday: true,
				IsOrderableOnThursday:  false,
				IsOrderableOnFriday:    true,
				IsOrderableOnSaturday:  false,
				IsOrderableOnSunday:    true,
			},
			IsActive: true,
		})
		ts.assertSuccess(req, expected)
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_InvalidPayload() {
	ts.Run("sellerId is less than 1", func() {
		req := &api.UpsertSupplierInfoRequest{}
		ts.assertError(req, "rpc error: code = InvalidArgument desc = invalid UpsertSupplierInfoRequest.Name: value length must be between 1 and 255 runes, inclusive")
	})
}

func (ts *UpsertSupplierInfoTestSuite) Test_HappyCase_InactiveSupplier() {
	ts.Run("Happy case, inactive supplier", func() {
		supplierId := int32(1000)
		ts.Faker.Supplier(ts.generateSupplier(supplierId), true)
		req := ts.populateReq(&api.UpsertSupplierInfoRequest{
			Id:                       supplierId,
			Code:                     helper.StringToProtoString(fmt.Sprintf("s%d", supplierId)),
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			SellerId:      1,
			RequestedById: "MJM56",
			IsActive:      helper.BoolToProtoBool(false),
		})
		expected := ts.populateResp(&api.SupplierInfo{
			Id:                       supplierId,
			Name:                     fmt.Sprintf("supplier %d", supplierId),
			Code:                     fmt.Sprintf("s%d", supplierId),
			TaxCode:                  fmt.Sprintf("tax%d", supplierId),
			PaymentAfterInvoiceDays:  helper.Int32ToProtoInt32(2),
			ExchangeableStartingDays: helper.Int32ToProtoInt32(0),
			Contacts: []*api.Contact{
				{
					CategoryIds: []int32{1, 2},
					Email:       helper.StringToProtoString(fmt.Sprintf("supplier%d@sup%d.com", supplierId, supplierId)),
					PhoneNumber: helper.StringToProtoString("113"),
				},
			},
			IsActive: false,
		})
		ts.assertSuccess(req, expected)
	})
}
