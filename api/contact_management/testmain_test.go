package contact_management

import (
	"context"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"gorm.io/gorm"

	"go.tekoapis.com/tekone/app/supplychain/supplier_service/tests"
	"go.tekoapis.com/tekone/library/log"
)

var (
	db     *gorm.DB
	logger log.LogRPlus
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var testContainer testcontainers.Container
	var exit bool
	testContainer, db, logger, exit = tests.SetUpTestMain(m, ctx)
	if exit {
		return
	}
	exitVal := m.Run()
	err := testContainer.Terminate(ctx)
	if err != nil {
		return
	}
	os.Exit(exitVal)
}
