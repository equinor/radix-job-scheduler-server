package jobs

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
	controller := jobController{handler: handler}
	controllerTestUtils := test.NewV2(&controller)
	return &controllerTestUtils
}

func TestGetRadixBatchSingleJobs(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		testBatchesWithJobs := []modelsv2.RadixBatch{createTestBatchWithSingleJob(), createTestBatchWithSingleJob(), createTestBatchWithSingleJob()}
		batchHandler.
			EXPECT().
			GetRadixBatchSingleJobs().
			Return(testBatchesWithJobs, nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v2/jobs")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatches []modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatches)
			assert.Len(t, returnedBatches, len(testBatchesWithJobs))
			for i := 0; i < len(returnedBatches); i++ {
				testBatch := testBatchesWithJobs[i]
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
	t.Run("status code 500", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchHandler.
			EXPECT().
			GetRadixBatchSingleJobs().
			Return(nil, apiErrors.NewUnknown(fmt.Errorf("unhandled error"))).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v2/jobs")
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

func TestCreateSingleJob(t *testing.T) {
	t.Run("valid payload body - successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		testBatchWithSingleJob := createTestBatchWithSingleJob()
		jobScheduleDescription := createJobScheduleDescription()
		batchHandler.
			EXPECT().
			CreateRadixBatchSingleJob(&jobScheduleDescription).
			Return(&testBatchWithSingleJob, nil).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/jobs", jobScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, testBatchWithSingleJob.Name, returnedBatch.Name)
			assert.Equal(t, testBatchWithSingleJob.CreationTime, returnedBatch.CreationTime)
			assert.Equal(t, testBatchWithSingleJob.Started, returnedBatch.Started)
			assert.Equal(t, testBatchWithSingleJob.Ended, returnedBatch.Ended)
			assert.Equal(t, testBatchWithSingleJob.Status, returnedBatch.Status)
			assert.Equal(t, testBatchWithSingleJob.Message, returnedBatch.Message)
			assert.Equal(t, len(testBatchWithSingleJob.JobStatuses), len(returnedBatch.JobStatuses))
			for i := 0; i < len(testBatchWithSingleJob.JobStatuses); i++ {
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Name, returnedBatch.JobStatuses[i].Name)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].CreationTime, returnedBatch.JobStatuses[i].CreationTime)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Started, returnedBatch.JobStatuses[i].Started)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Ended, returnedBatch.JobStatuses[i].Ended)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Status, returnedBatch.JobStatuses[i].Status)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Message, returnedBatch.JobStatuses[i].Message)
			}
		}
	})
	t.Run("valid payload body - error from MaintainHistoryLimit should not fail request", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		testBatchWithSingleJob := createTestBatchWithSingleJob()
		jobScheduleDescription := createJobScheduleDescription()
		batchHandler.
			EXPECT().
			CreateRadixBatchSingleJob(&jobScheduleDescription).
			Return(&testBatchWithSingleJob, nil).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(apiErrors.NewUnknown(fmt.Errorf("unhandled error"))).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/jobs", jobScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, testBatchWithSingleJob.Name, returnedBatch.Name)
			assert.Equal(t, testBatchWithSingleJob.CreationTime, returnedBatch.CreationTime)
			assert.Equal(t, testBatchWithSingleJob.Started, returnedBatch.Started)
			assert.Equal(t, testBatchWithSingleJob.Ended, returnedBatch.Ended)
			assert.Equal(t, testBatchWithSingleJob.Status, returnedBatch.Status)
			assert.Equal(t, testBatchWithSingleJob.Message, returnedBatch.Message)
			assert.Equal(t, len(testBatchWithSingleJob.JobStatuses), len(returnedBatch.JobStatuses))
			for i := 0; i < len(testBatchWithSingleJob.JobStatuses); i++ {
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Name, returnedBatch.JobStatuses[i].Name)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].CreationTime, returnedBatch.JobStatuses[i].CreationTime)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Started, returnedBatch.JobStatuses[i].Started)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Ended, returnedBatch.JobStatuses[i].Ended)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Status, returnedBatch.JobStatuses[i].Status)
				assert.Equal(t, testBatchWithSingleJob.JobStatuses[i].Message, returnedBatch.JobStatuses[i].Message)
			}
		}
	})
	t.Run("status code 500", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobScheduleDescription := createJobScheduleDescription()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateRadixBatchSingleJob(&jobScheduleDescription).
			Return(nil, apiErrors.NewUnknown(fmt.Errorf("unhandled error"))).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			AnyTimes()

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/jobs", jobScheduleDescription)
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
	t.Run("handler returning NotFound error - 404 not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobScheduleDescription := createJobScheduleDescription()
		batchHandler := batchMock.NewMockHandler(ctrl)
		anyKind, anyName := "anyKind", "anyName"
		batchHandler.
			EXPECT().
			CreateRadixBatchSingleJob(gomock.Any()).
			Return(nil, apiErrors.NewNotFound(anyKind, anyName)).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			AnyTimes()

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "api/v2/jobs", jobScheduleDescription)
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

func TestStopSingleJob(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		batchName := "anybatch"
		jobName := "anyjob"
		batchHandler.
			EXPECT().
			StopRadixBatchJob(batchName, jobName).
			Return(nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("api/v2/jobs/%s/stop", batchName))
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
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("api/v2/jobs/%s", batchName))
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
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("api/v2/jobs/%s", batchName))
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

func createJobScheduleDescription() models.JobScheduleDescription {
	return models.JobScheduleDescription{JobId: "job-1", Payload: "some payload"}
}

func createTestBatchWithSingleJob() modelsv2.RadixBatch {
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
				Name:         "job-name2",
				CreationTime: commonUtils.FormatTimestamp(now.Add(time.Minute * 20)),
				JobId:        "job-0",
				Started:      commonUtils.FormatTimestamp(now.Add(time.Minute * 30)),
				Ended:        commonUtils.FormatTimestamp(now.Add(time.Hour * 40)),
				Status:       "some job2 status",
				Message:      "some job2 message",
			},
		},
	}
}
