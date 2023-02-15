package batch

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	commonUtils "github.com/equinor/radix-common/utils"
	"github.com/equinor/radix-common/utils/pointers"
	"github.com/equinor/radix-job-scheduler-server/api/utils/test"
	apiErrors "github.com/equinor/radix-job-scheduler/api/errors"
	api "github.com/equinor/radix-job-scheduler/api/v2"
	batchMock "github.com/equinor/radix-job-scheduler/api/v2/mock"
	"github.com/equinor/radix-job-scheduler/models"
	modelsv2 "github.com/equinor/radix-job-scheduler/models/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func setupTest(handler api.Handler) *test.ControllerTestUtils {
	controller := batchController{handler: handler}
	controllerTestUtils := test.NewV2(&controller)
	return &controllerTestUtils
}

func TestGetBatches(t *testing.T) {
	t.Run("Get batches - success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		testBatches := []modelsv2.RadixBatch{createTestBatch(), createTestBatch(), createTestBatch()}
		batchHandler.
			EXPECT().
			GetRadixBatches().
			Return(testBatches, nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v2/batches")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatches []modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatches)
			assert.Len(t, returnedBatches, len(testBatches))
			for i := 0; i < len(returnedBatches); i++ {
				testBatch := testBatches[i]
				assert.Equal(t, testBatch.Name, returnedBatches[i].Name)
				assert.Equal(t, testBatch.CreationTime, returnedBatches[i].CreationTime)
				assert.Equal(t, testBatch.Started, returnedBatches[i].Started)
				assert.Equal(t, testBatch.Ended, returnedBatches[i].Ended)
				assert.Equal(t, testBatch.Status, returnedBatches[i].Status)
				assert.Equal(t, testBatch.Message, returnedBatches[i].Message)
				assert.Equal(t, len(testBatch.JobStatuses), len(returnedBatches[i].JobStatuses))
				for i := 0; i < len(testBatch.JobStatuses); i++ {
					assert.Equal(t, testBatch.JobStatuses[i].Name, returnedBatches[i].JobStatuses[i].Name)
					assert.Equal(t, testBatch.JobStatuses[i].CreationTime, returnedBatches[i].JobStatuses[i].CreationTime)
					assert.Equal(t, testBatch.JobStatuses[i].Started, returnedBatches[i].JobStatuses[i].Started)
					assert.Equal(t, testBatch.JobStatuses[i].Ended, returnedBatches[i].JobStatuses[i].Ended)
					assert.Equal(t, testBatch.JobStatuses[i].Status, returnedBatches[i].JobStatuses[i].Status)
					assert.Equal(t, testBatch.JobStatuses[i].Message, returnedBatches[i].JobStatuses[i].Message)
				}
			}
		}
	})
	t.Run("Get batches - status code 500", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchHandler.
			EXPECT().
			GetRadixBatches().
			Return(nil, apiErrors.NewUnknown(fmt.Errorf("unhandled error"))).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v2/batches")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusInternalServerError, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonUnknown, returnedStatus.Reason)
		}
	})
}

func TestGetBatch(t *testing.T) {
	t.Run("Get batch - success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		testBatch := createTestBatch()
		batchHandler.
			EXPECT().
			GetRadixBatch(testBatch.Name).
			Return(&testBatch, nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("api/v2/batches/%s", testBatch.Name))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, testBatch.Name, returnedBatch.Name)
			assert.Equal(t, testBatch.CreationTime, returnedBatch.CreationTime)
			assert.Equal(t, testBatch.Started, returnedBatch.Started)
			assert.Equal(t, testBatch.Ended, returnedBatch.Ended)
			assert.Equal(t, testBatch.Status, returnedBatch.Status)
			assert.Equal(t, testBatch.Message, returnedBatch.Message)
			assert.Equal(t, len(testBatch.JobStatuses), len(returnedBatch.JobStatuses))
			for i := 0; i < len(testBatch.JobStatuses); i++ {
				assert.Equal(t, testBatch.JobStatuses[i].Name, returnedBatch.JobStatuses[i].Name)
				assert.Equal(t, testBatch.JobStatuses[i].CreationTime, returnedBatch.JobStatuses[i].CreationTime)
				assert.Equal(t, testBatch.JobStatuses[i].Started, returnedBatch.JobStatuses[i].Started)
				assert.Equal(t, testBatch.JobStatuses[i].Ended, returnedBatch.JobStatuses[i].Ended)
				assert.Equal(t, testBatch.JobStatuses[i].Status, returnedBatch.JobStatuses[i].Status)
				assert.Equal(t, testBatch.JobStatuses[i].Message, returnedBatch.JobStatuses[i].Message)
			}
		}
	})
	t.Run("Get batch - status code 500", func(t *testing.T) {
		t.Parallel()
		batchName := "some-batch-name"
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchHandler.
			EXPECT().
			GetRadixBatch(batchName).
			Return(nil, apiErrors.NewUnknown(fmt.Errorf("unhandled error"))).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("api/v2/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusInternalServerError, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonUnknown, returnedStatus.Reason)
		}
	})
}

func TestCreateBatch(t *testing.T) {
	t.Run("valid payload body - successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		testBatch := createTestBatch()
		batchScheduleDescription := createBatchScheduleDescription()
		batchHandler.
			EXPECT().
			CreateRadixBatch(&batchScheduleDescription).
			Return(&testBatch, nil).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/batches", batchScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, testBatch.Name, returnedBatch.Name)
			assert.Equal(t, testBatch.CreationTime, returnedBatch.CreationTime)
			assert.Equal(t, testBatch.Started, returnedBatch.Started)
			assert.Equal(t, testBatch.Ended, returnedBatch.Ended)
			assert.Equal(t, testBatch.Status, returnedBatch.Status)
			assert.Equal(t, testBatch.Message, returnedBatch.Message)
			assert.Equal(t, len(testBatch.JobStatuses), len(returnedBatch.JobStatuses))
			for i := 0; i < len(testBatch.JobStatuses); i++ {
				assert.Equal(t, testBatch.JobStatuses[i].Name, returnedBatch.JobStatuses[i].Name)
				assert.Equal(t, testBatch.JobStatuses[i].CreationTime, returnedBatch.JobStatuses[i].CreationTime)
				assert.Equal(t, testBatch.JobStatuses[i].Started, returnedBatch.JobStatuses[i].Started)
				assert.Equal(t, testBatch.JobStatuses[i].Ended, returnedBatch.JobStatuses[i].Ended)
				assert.Equal(t, testBatch.JobStatuses[i].Status, returnedBatch.JobStatuses[i].Status)
				assert.Equal(t, testBatch.JobStatuses[i].Message, returnedBatch.JobStatuses[i].Message)
			}
		}
	})
	t.Run("valid payload body - error from MaintainHistoryLimit should not fail request", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		testBatch := createTestBatch()
		batchScheduleDescription := createBatchScheduleDescription()
		batchHandler.
			EXPECT().
			CreateRadixBatch(&batchScheduleDescription).
			Return(&testBatch, nil).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(apiErrors.NewUnknown(fmt.Errorf("unhandled error"))).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/batches", batchScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, testBatch.Name, returnedBatch.Name)
			assert.Equal(t, testBatch.CreationTime, returnedBatch.CreationTime)
			assert.Equal(t, testBatch.Started, returnedBatch.Started)
			assert.Equal(t, testBatch.Ended, returnedBatch.Ended)
			assert.Equal(t, testBatch.Status, returnedBatch.Status)
			assert.Equal(t, testBatch.Message, returnedBatch.Message)
			assert.Equal(t, len(testBatch.JobStatuses), len(returnedBatch.JobStatuses))
			for i := 0; i < len(testBatch.JobStatuses); i++ {
				assert.Equal(t, testBatch.JobStatuses[i].Name, returnedBatch.JobStatuses[i].Name)
				assert.Equal(t, testBatch.JobStatuses[i].CreationTime, returnedBatch.JobStatuses[i].CreationTime)
				assert.Equal(t, testBatch.JobStatuses[i].Started, returnedBatch.JobStatuses[i].Started)
				assert.Equal(t, testBatch.JobStatuses[i].Ended, returnedBatch.JobStatuses[i].Ended)
				assert.Equal(t, testBatch.JobStatuses[i].Status, returnedBatch.JobStatuses[i].Status)
				assert.Equal(t, testBatch.JobStatuses[i].Message, returnedBatch.JobStatuses[i].Message)
			}
		}
	})
	t.Run("status code 500", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := models.BatchScheduleDescription{
			JobScheduleDescriptions: []models.JobScheduleDescription{{}, {}},
		}
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateRadixBatch(&batchScheduleDescription).
			Return(nil, apiErrors.NewUnknown(fmt.Errorf("unhandled error"))).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			AnyTimes()

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/batches", batchScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusInternalServerError, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonUnknown, returnedStatus.Reason)
		}
	})
	t.Run("invalid request body - unprocessable", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := struct{ JobScheduleDescriptions interface{} }{JobScheduleDescriptions: struct{}{}}
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateRadixBatch(gomock.Any()).
			Times(0)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			AnyTimes()

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/batches", batchScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusUnprocessableEntity, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusUnprocessableEntity, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonInvalid, returnedStatus.Reason)
			assert.Equal(t, apiErrors.InvalidMessage("BatchScheduleDescription"), returnedStatus.Message)
		}
	})
	t.Run("handler returning NotFound error - 404 not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := createBatchScheduleDescription()
		batchHandler := batchMock.NewMockHandler(ctrl)
		anyKind, anyName := "anyKind", "anyName"
		batchHandler.
			EXPECT().
			CreateRadixBatch(gomock.Any()).
			Return(nil, apiErrors.NewNotFound(anyKind, anyName)).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			AnyTimes()

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/batches", batchScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusNotFound, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage(anyKind, anyName), returnedStatus.Message)
		}
	})
}

func TestDeleteBatch(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchName := "anybatch"
		batchHandler.
			EXPECT().
			DeleteRadixBatch(batchName).
			Return(nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("api/v2/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusOK, returnedStatus.Code)
			assert.Equal(t, models.StatusSuccess, returnedStatus.Status)
			assert.Empty(t, returnedStatus.Reason)
		}
	})
	t.Run("404 not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchName := "anybatch"
		batchHandler.
			EXPECT().
			DeleteRadixBatch(batchName).
			Return(apiErrors.NewNotFound("batch", batchName)).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("api/v2/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusNotFound, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage("batch", batchName), returnedStatus.Message)
		}
	})
	t.Run("500 internal server error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchName := "anybatch"
		batchHandler.
			EXPECT().
			DeleteRadixBatch(batchName).
			Return(apiErrors.NewUnknown(fmt.Errorf("any error"))).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("api/v2/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusInternalServerError, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonUnknown, returnedStatus.Reason)
		}
	})
}

func createBatchScheduleDescription() models.BatchScheduleDescription {
	return models.BatchScheduleDescription{
		JobScheduleDescriptions: []models.JobScheduleDescription{{}, {}},
	}
}

func createTestBatch() modelsv2.RadixBatch {
	now := time.Now()
	return modelsv2.RadixBatch{
		Name:         "some-batch-name",
		CreationTime: commonUtils.FormatTimestamp(now),
		Started:      commonUtils.FormatTime(pointers.Ptr(metav1.NewTime(now.Add(time.Minute)))),
		Ended:        commonUtils.FormatTime(pointers.Ptr(metav1.NewTime(now.Add(time.Minute * 50)))),
		Status:       "some status",
		Message:      "test message",
		JobStatuses: []modelsv2.RadixBatchJobStatus{
			{
				Name:         "job-name1",
				CreationTime: commonUtils.FormatTimestamp(now.Add(time.Minute * 2)),
				JobId:        "",
				Started:      commonUtils.FormatTimestamp(now.Add(time.Minute * 3)),
				Ended:        commonUtils.FormatTimestamp(now.Add(time.Hour * 4)),
				Status:       "some job1 status",
				Message:      "some job1 message",
			},
			{
				Name:         "job-name2",
				CreationTime: commonUtils.FormatTimestamp(now.Add(time.Minute * 20)),
				JobId:        "",
				Started:      commonUtils.FormatTimestamp(now.Add(time.Minute * 30)),
				Ended:        commonUtils.FormatTimestamp(now.Add(time.Hour * 40)),
				Status:       "some job2 status",
				Message:      "some job2 message",
			},
		},
	}
}
