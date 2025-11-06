package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	migrateV4 "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"

	"go.tekoapis.com/tekone/app/supplychain/supplier_service/config"
	grpcCtx "go.tekoapis.com/tekone/library/grpc/ctx"
	grpcLogger "go.tekoapis.com/tekone/library/grpc/logging"
	"go.tekoapis.com/tekone/library/log"
)

const (
	runningEnv             = "RUNNING_ENV"
	local                  = "LOCAL"
	timeOutCreateContainer = 2
	dbTestDatabase         = "test"
	dbTestUserName         = "root"
	dbTestPassword         = "root"
	dbTestOption           = "?parseTime=true&timeout=90s"
	maxLookUpDir           = 20
)

func SetUpTestMain(m *testing.M, ctx context.Context) (testcontainers.Container, *gorm.DB, log.LogRPlus, bool) {
	// if local, run test and return
	if isLocalEnv() {
		exitVal := m.Run()
		os.Exit(exitVal)
		return nil, nil, nil, true
	}
	// else init docker db
	testContainer, gormDb, logService, err := setupMySqlContainer(ctx)
	if err != nil {
		panic(err)
	}
	return testContainer, gormDb, logService, false
}

func isLocalEnv() bool {
	return os.Getenv(runningEnv) == local
}

func setupMySqlContainer(ctx context.Context) (testcontainers.Container, *gorm.DB, log.LogRPlus, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, timeOutCreateContainer*time.Minute)
	defer cancel()
	// create container
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": dbTestPassword,
			"MYSQL_DATABASE":      dbTestDatabase,
			"TZ":                  "UTC",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("port: 3306  MySQL Community Server"),
			wait.ForListeningPort("3306/tcp"),
		),
	}
	containerDb, err := testcontainers.GenericContainer(ctxTimeout, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// prepare env
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, nil, err
	}

	containerHost, err := containerDb.Host(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	//Getting mapped port from started container
	mappedPort, err := containerDb.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return nil, nil, nil, err
	}
	port, err := strconv.Atoi(mappedPort.Port())
	if err != nil {
		return nil, nil, nil, err
	}

	cfg.MySQL.Host = containerHost
	cfg.MySQL.Database = dbTestDatabase
	cfg.MySQL.Port = port
	cfg.MySQL.Username = dbTestUserName
	cfg.MySQL.Password = dbTestPassword
	cfg.MySQL.Options = dbTestOption

	databaseURL := cfg.MySQL.String()

	currentDir, err := os.Getwd()
	if err != nil {
		return nil, nil, nil, err
	}

	var migrationDir string
	for i := 0; i < maxLookUpDir; i++ {
		serviceRootDir := currentDir + strings.Repeat("/..", i)
		migrationDir = filepath.Join(serviceRootDir, "./sql/migrations")
		_, err = os.Stat(migrationDir)
		if err == nil {
			break
		}
	}

	// migrate
	sourceURL := fmt.Sprintf("file://%s", migrationDir)
	migrate, err := migrateV4.New(sourceURL, databaseURL)

	if err != nil {
		return nil, nil, nil, err
	}

	if err = migrate.Up(); err != nil && err != migrateV4.ErrNoChange {
		return nil, nil, nil, err
	}

	logger := cfg.Log.MustBuildLogR()
	serviceLog := log.NewLogRPlus(
		logger,
		grpcCtx.ExtractServerCtx,
		grpcLogger.TagsToFields,
	)

	db, err := gorm.Open(mysql.Open(cfg.MySQL.DSN()), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Info),
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return containerDb, db, serviceLog, nil
}
