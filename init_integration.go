package tests

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcValidator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	grpcPrometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/supplier_service/api"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/config"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/constant"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/faker"
	"go.tekoapis.com/tekone/app/supplychain/supplier_service/pkg/helper"
	grpcCtx "go.tekoapis.com/tekone/library/grpc/ctx"
	grpcLogger "go.tekoapis.com/tekone/library/grpc/logging"
	"go.tekoapis.com/tekone/library/log"
)

const (
	dbHostEnv              = "MYSQL_HOST"
	dbPortEnv              = "MYSQL_PORT"
	dbUserNameEnv          = "MYSQL_USERNAME"
	dbPwEnv                = "MYSQL_PASSWORD"
	dbDatabaseEnv          = "MYSQL_DATABASE"
	dbOptionEnv            = "MYSQL_OPTIONS"
	dbHostDefaultLocal     = "127.0.0.1"
	dbPortDefaultLocal     = "3306"
	dbUserDefaultLocal     = "root"
	dbPwDefaultLocal       = "secret"
	dbDatabaseDefaultLocal = "supplier"
	dbOptionDefaultLocal   = "?parseTime=true&timeout=90s"
	bufSize                = 1024 * 1024
)

type TestSuite struct {
	suite.Suite
	Client  api.SupplierServiceClient
	DB      *gorm.DB
	Faker   faker.Faker
	Context context.Context
}

var (
	lis *bufconn.Listener
)

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func InitIntegrationTestMain(t *testing.T, db *gorm.DB, log log.LogRPlus) (*gorm.DB, log.LogRPlus) {
	if isLocalEnv() {
		fmt.Println("Start local database environment")
		return initLocalIntegrationTest(t)
	}
	return db, log
}

func initLocalIntegrationTest(t *testing.T) (*gorm.DB, log.LogRPlus) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatal("Can't load config", err)
	}
	logger := cfg.Log.MustBuildLogR()
	serviceLog := log.NewLogRPlus(
		logger,
		grpcCtx.ExtractServerCtx,
		grpcLogger.TagsToFields,
	)

	dbHost := os.Getenv(dbHostEnv)
	if dbHost == constant.EmptyString {
		dbHost = dbHostDefaultLocal
	}
	dbPort := os.Getenv(dbPortEnv)
	if dbPort == constant.EmptyString {
		dbPort = dbPortDefaultLocal
	}
	dbUserName := os.Getenv(dbUserNameEnv)
	if dbUserName == constant.EmptyString {
		dbUserName = dbUserDefaultLocal
	}
	dbPwEnv := os.Getenv(dbPwEnv)
	if dbPwEnv == constant.EmptyString {
		dbPwEnv = dbPwDefaultLocal
	}
	dbDatabase := os.Getenv(dbDatabaseEnv)
	if dbDatabase == constant.EmptyString {
		dbDatabase = dbDatabaseDefaultLocal
	}
	dbOption := os.Getenv(dbOptionEnv)
	if dbOption == constant.EmptyString {
		dbOption = dbOptionDefaultLocal
	}
	port, err := strconv.Atoi(dbPort)
	if err != nil {
		t.Fatal("Can't load env", err)
	}

	cfg.MySQL.Host = dbHost
	cfg.MySQL.Database = dbDatabase
	cfg.MySQL.Port = port
	cfg.MySQL.Username = dbUserName
	cfg.MySQL.Password = dbPwEnv
	cfg.MySQL.Options = dbOption
	db, err := helper.MustConnectMySQL(cfg.MySQL, false, helper.MysqlDefault)
	if err != nil {
		panic(err)
	}
	return db, serviceLog
}

func (s *TestSuite) InitServerAndClient(t *testing.T, srv api.SupplierServiceServer) (*grpc.ClientConn, *grpc.Server) {
	lis = bufconn.Listen(bufSize)
	server := grpc.NewServer(
		grpcMiddleware.WithUnaryServerChain(
			grpcPrometheus.UnaryServerInterceptor,
			grpcValidator.UnaryServerInterceptor(),
		),
	)
	api.RegisterSupplierServiceServer(server, srv)
	go func() {
		if err := server.Serve(lis); err != nil {
			panic("Server exited with error")
		}
	}()
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	s.Client = api.NewSupplierServiceClient(conn)
	return conn, server
}
