package tests

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcValidator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	grpcPrometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/api"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/config"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/errorz"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	grpcCtx "go.tekoapis.com/tekone/library/grpc/ctx"
	grpcLogger "go.tekoapis.com/tekone/library/grpc/logging"
	"go.tekoapis.com/tekone/library/log"
)

type (
	TestSuite struct {
		mu sync.RWMutex
		suite.Suite
		suite.TearDownAllSuite
		suite.SetupAllSuite
		suiteName            string
		IssuesKey            string
		Folder               string
		ApiName              string
		keys                 []string
		tests                map[string][]*testInfo
		DemandPlanningClient api.DemandPlanningServiceClient
		DbName               string
		Context              context.Context
	}

	testInfo struct {
		name   string
		status string
		run    bool
	}
)

const (
	submitTestEnv          = "submit_test"
	removeOldTestEnv       = "remove_old_test"
	timeOutCreateContainer = 3

	dbHostEnv     = "MYSQL_HOST"
	dbPortEnv     = "MYSQL_PORT"
	dbUserNameEnv = "MYSQL_USERNAME"
	dbPwEnv       = "MYSQL_PASSWORD"
	dbDatabaseEnv = "MYSQL_DATABASE"
	dbOptionEnv   = "MYSQL_OPTIONS"
)

func (s *TestSuite) Run(name string, subtest func()) bool {
	oldT := s.T()
	defer s.endRun(oldT, name)
	return oldT.Run(name, func(t *testing.T) {
		s.SetT(t)
		subtest()
	})
}

func (s *TestSuite) endRun(t *testing.T, name string) {
	s.SetT(t)
	testName := fmt.Sprintf("%s/%s", t.Name(), name)
	s.addTest(testName, true)
}

func (s *TestSuite) SetupSuite() {
	s.suiteName = s.T().Name()
	s.keys = strings.Split(s.IssuesKey, ",")
	s.tests = make(map[string][]*testInfo)
}

func (s *TestSuite) TearDownSuite() {
	submit := os.Getenv("submit_test")
	if submit == "1" {
		for issueKey, tests := range s.tests {
			runTests := s.getRunTests(tests)
			s.pushTests(issueKey, runTests)
		}
	}
}

func (s *TestSuite) AfterTest(_, testName string) {
	s.addTest(testName, false)
}

func (s *TestSuite) getRunTests(tests []*testInfo) []*testInfo {
	var runTests []*testInfo
	for _, t := range tests {
		if t.run {
			runTests = append(runTests, t)
		}
	}
	for _, t := range tests {
		if t.run {
			continue
		}
		run := false
		for _, rt := range runTests {
			if strings.Contains(rt.name, fmt.Sprintf("%s/", t.name)) {
				run = true
				break
			}
		}
		if !run {
			runTests = append(runTests, t)
		}
	}
	return runTests
}

func (s *TestSuite) pushTests(issueKey string, tests []*testInfo) {
	pushTests := make([]*TestCase, 0, len(tests))
	if s.IssuesKey == "" {
		s.IssuesKey = os.Getenv("issue_key")
	}
	if s.IssuesKey == "" {
		panic("could not found IssuesKey")
	}
	if s.Folder == "" {
		s.Folder = s.IssuesKey
	}
	if s.ApiName == "" {
		s.ApiName = s.IssuesKey
	}
	for _, t := range tests {
		name := strings.Replace(t.name, fmt.Sprintf("%s/", s.suiteName), "", 1)
		if s.ApiName != "" && strings.Contains(name, "/") {
			a := strings.Split(name, "/")
			name = strings.Replace(name, a[0], "", 1)
			name = fmt.Sprintf("[%s]", s.ApiName) + strings.ReplaceAll(name, "/", "-")
		} else {
			name = strings.Replace(name, "Test", "", 1)
		}
		pushTests = append(pushTests, &TestCase{
			Name:   name,
			Status: t.status,
		})
	}
	GetTest().PushTests(issueKey, s.ApiName, s.Folder, pushTests)
}

func (s *TestSuite) addTest(testName string, run bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := &testInfo{
		name:   testName,
		status: "Pass",
		run:    run,
	}
	if s.T().Failed() {
		item.status = "Fail"
	}
	checkName := testName + "/"
	for _, issue := range s.keys {
		oldTests, exist := s.tests[issue]
		var tests []*testInfo
		if exist {
			// Check whether test has children
			isParent := false
			for _, t := range oldTests {
				if strings.Contains(t.name, checkName) {
					isParent = true
				}
				// Move parent testcase if a child testcase is added
				if !strings.Contains(testName, t.name+"/") {
					tests = append(tests, t)
				}
			}
			if !isParent {
				tests = append(tests, item)
			}
		} else {
			tests = append(tests, item)
		}
		s.tests[issue] = tests
	}
}

func (s *TestSuite) TruncateDatabase(db *gorm.DB, tableName string) error {
	if isLocalEnv() {
		s.DeleteAllTables(db, tableName)
		s.ResetAutoIncrement(db, tableName)
		return nil
	}
	return db.Exec(fmt.Sprintf("TRUNCATE TABLE  %s;", tableName)).Error

}

func (s *TestSuite) TruncateDatabases(db *gorm.DB, tableNames ...string) {
	for _, tableName := range tableNames {
		err := s.TruncateDatabase(db, tableName)
		if err != nil {
			panic(fmt.Sprintf("Cann't truncate table %s", tableName))
		}
	}
}

func (s *TestSuite) InitIntegrationTestMain(t *testing.T, db *gorm.DB, log log.LogRPlus) (*gorm.DB, log.LogRPlus) {
	if isLocalEnv() {
		fmt.Println("Start local database environment")
		return s.InitLocalIntegrationTest(t)
	}
	return db, log
}

func RunSuite(t *testing.T, s suite.TestingSuite, submitJiraTest bool, removeOldJiraTest bool) {
	if err := configEnv(submitJiraTest, removeOldJiraTest); err != nil {
		t.Fatal("Can't create environment")
	}
	suite.Run(t, s)
	if err := unConfigEnv(); err != nil {
		t.Fatal("Can't unset environment")
	}
}

var lis *bufconn.Listener

const bufSize = 1024 * 1024

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func (s *TestSuite) InitServerAndClient(t *testing.T, srv api.DemandPlanningServiceServer) (*grpc.ClientConn, *grpc.Server) {
	lis = bufconn.Listen(bufSize)
	server := grpc.NewServer(
		grpcMiddleware.WithUnaryServerChain(
			grpcPrometheus.UnaryServerInterceptor,
			grpcValidator.UnaryServerInterceptor(),
			handleErrorInterceptor(),
		),
	)
	api.RegisterDemandPlanningServiceServer(server, srv)
	go func() {
		if err := server.Serve(lis); err != nil {
			panic("Server exited with error")
		}
	}()
	s.Context = context.Background()
	conn, err := grpc.DialContext(s.Context, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	s.DemandPlanningClient = api.NewDemandPlanningServiceClient(conn)
	return conn, server
}

func configEnv(submitJiraTest bool, removeOldJiraTest bool) error {
	// The test will be pushed automatically to jira after test if env "submit_test"
	// is set to "1". If you don't want to push automatically, just comment this out
	submitTest := "0"
	if submitJiraTest {
		submitTest = "1"
	}
	err := os.Setenv(submitTestEnv, submitTest)
	if err != nil {
		return err
	}

	// Set "remove_old_test" to "1" will delete all the older testcase in the issue before
	removeOldTest := "0"
	if removeOldJiraTest {
		removeOldTest = "1"
	}
	err = os.Setenv(removeOldTestEnv, removeOldTest)
	if err != nil {
		return err
	}

	return nil
}

func unConfigEnv() error {
	err := os.Unsetenv(submitTestEnv)
	if err != nil {
		return err
	}
	err = os.Unsetenv(removeOldTestEnv)
	if err != nil {
		return err
	}

	return nil
}

func (s *TestSuite) InitLocalIntegrationTest(t *testing.T) (*gorm.DB, log.LogRPlus) {
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
	if dbHost == "" {
		dbHost = "127.0.0.1"
	}
	dbPort := os.Getenv(dbPortEnv)
	if dbPort == "" {
		dbPort = "3306"
	}
	dbUserName := os.Getenv(dbUserNameEnv)
	if dbUserName == "" {
		dbUserName = "root"
	}
	dbPwEnv := os.Getenv(dbPwEnv)
	if dbPwEnv == "" {
		dbPwEnv = "secret"
	}
	dbDatabase := os.Getenv(dbDatabaseEnv)
	if dbDatabase == "" {
		dbDatabase = "sc_demand_planning_service_integration_test"
	}
	dbOption := os.Getenv(dbOptionEnv)
	if dbOption == "" {
		dbOption = "?parseTime=true&timeout=90s"
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
	s.DbName = dbDatabase
	db := helper.MySQLIntegrationTest(&cfg.MySQL)
	return db, serviceLog
}

func (s *TestSuite) DeleteAllTables(db *gorm.DB, tNames ...string) {
	err := db.Transaction(func(tx *gorm.DB) error {
		for _, name := range tNames {
			e := tx.Exec(fmt.Sprintf("DELETE from  %s", name)).Error
			if e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		s.T().Fatalf(fmt.Sprintf("Can't delete %v", err.Error()))
	}
}

func (s *TestSuite) ResetAutoIncrement(db *gorm.DB, tName string) {
	err := db.Exec(fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = 1", tName)).Error
	if err != nil {
		s.T().Fatalf(fmt.Sprintf("Can't reset %v", err.Error()))
	}
}

func handleErrorInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (res interface{}, err error) {
		resp, err := handler(ctx, req)
		if err != nil && errors.As(err, &errorz.DPSErrorNoAlert{}) {
			for errors.Unwrap(err) != nil {
				wrappedErr := err
				err = errors.Unwrap(err)

				switch wrappedErr.(type) {
				case errorz.DPSErrorNoAlert:
					break
				}
			}
		}
		return resp, err
	}
}
