package schedule_job

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/api"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/config"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/adapter/kafka"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/model"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/provider"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/scheduler"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/internal/service"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/constant"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/pkg/helper"
	"go.tekoapis.com/tekone/app/supplychain/demand_planning_service/tests"
	"go.tekoapis.com/tekone/library/test/monkey"
)

type jobSyncSiteSkuDemandTestSuite struct {
	tests.TestSuite
	handler *scheduler.Handler
	ctx     context.Context

	fixedTime   time.Time
	isSentKafka bool
}

func TestJobSyncSiteSkuDemand(t *testing.T) {
	ts := &jobSyncSiteSkuDemandTestSuite{}
	ts.isSentKafka = false
	ts.ctx = context.Background()
	ts.fixedTime = time.Date(2021, 8, 30, 0, 0, 0, 0, time.UTC)

	kafkaPublisher := &kafka.Publisher{}
	monkey.PatchInstanceMethod(reflect.TypeOf(kafkaPublisher), "PushKafkaEvent",
		func(kafka *kafka.Publisher, ctx context.Context, payload proto.Message, topic string) error {
			ts.isSentKafka = true
			return nil
		})

	ts.handler = scheduler.NewHandler(&service.Service{
		Config: &config.Config{
			SchedulerConfig: config.SchedulerConfig{
				DefaultMaxRetry: 0,
			},
		},
		KafkaPublisher: kafkaPublisher,
	})

	ts.tearDown()
	defer func() {
		ts.tearDown()
		monkey.UnpatchAll()
	}()

	suite.Run(t, ts)
}

func (ts *jobSyncSiteSkuDemandTestSuite) tearDown() {
	ts.isSentKafka = false
}

func (ts *jobSyncSiteSkuDemandTestSuite) assert(job model.ScheduleJob, isSentKafka bool) {
	defer ts.tearDown()

	_, err := ts.handler.Handle(ts.ctx, job)
	assert.Nil(ts.T(), err)
	assert.Equal(ts.T(), isSentKafka, ts.isSentKafka)
}

func (ts *jobSyncSiteSkuDemandTestSuite) Test200_ReqSyncSiteSkuDemand_ThenReturnSuccess() {
	payload := &provider.PayloadUpdateSkuDemand{
		Source: constant.UpdatedByScheduler,
		PayloadKafka: &api.UpsertDemandForecastEvent{
			EventKey: "key",
			SellerId: 1,
			DemandForecastItems: []*api.UpsertDemandForecastEvent_DemandForecastItem{
				{
					SiteId:             1,
					Sku:                "sku",
					DemandForecastSale: 1.2,
				},
				{
					SiteId:             1,
					Sku:                "sku2",
					DemandForecastSale: 2.2,
				},
			},
		},
	}
	payloadStr, err := json.Marshal(payload)
	assert.Nil(ts.T(), err)
	ts.assert(model.ScheduleJob{
		Type:      model.ScheduleJobTypeSyncSiteSkuDemand,
		Status:    int32(model.SchedulerJobStatusUnProcessed),
		Payload:   helper.StringToSqlString(string(payloadStr)),
		CreatedAt: ts.fixedTime,
		UpdatedAt: ts.fixedTime,
	}, true)
}
