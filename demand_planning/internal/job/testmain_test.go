package job

import (
	"context"
	"os"
	"testing"

	"gorm.io/gorm"

	"go.tekoapis.com/tekone/library/integrationtest"
	"go.tekoapis.com/tekone/library/log"
)

var (
	gormDb     *gorm.DB
	logService log.LogRPlus
	testData   *integrationtest.TestMainDataStruct
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	var exitVal int
	testData, err = integrationtest.SetUpTestMain(ctx, m, integrationtest.IntegrationTestOptions{
		IsDisableCheckForeignKey: true,
		IsDisableUniqueKey:       true,
		DatabaseType:             integrationtest.DatabaseTypeMySql,
	})
	gormDb, logService = testData.Db, testData.LogService
	defer func() {
		testData.DeferFunction()
		if err != nil {
			panic(err)
		}
		os.Exit(exitVal)
	}()
	if err != nil {
		return
	}
	exitVal = m.Run()
}
