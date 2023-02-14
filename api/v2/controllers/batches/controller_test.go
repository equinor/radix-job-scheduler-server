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
		now := time.Now()
		radixBatch := modelsv2.RadixBatch{
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
		batchHandler.
			EXPECT().
			GetRadixBatches().
			Return([]modelsv2.RadixBatch{radixBatch}, nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v2/batches")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatches []modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatches)
			assert.Len(t, returnedBatches, 1)
			assert.Equal(t, radixBatch.Name, returnedBatches[0].Name)
			assert.Equal(t, radixBatch.CreationTime, returnedBatches[0].CreationTime)
			assert.Equal(t, radixBatch.Started, returnedBatches[0].Started)
			assert.Equal(t, radixBatch.Ended, returnedBatches[0].Ended)
			assert.Equal(t, radixBatch.Status, returnedBatches[0].Status)
			assert.Equal(t, radixBatch.Message, returnedBatches[0].Message)
			assert.Equal(t, len(radixBatch.JobStatuses), len(returnedBatches[0].JobStatuses))
			for i := 0; i < len(radixBatch.JobStatuses); i++ {
				assert.Equal(t, radixBatch.JobStatuses[i].Name, returnedBatches[0].JobStatuses[i].Name)
				assert.Equal(t, radixBatch.JobStatuses[i].CreationTime, returnedBatches[0].JobStatuses[i].CreationTime)
				assert.Equal(t, radixBatch.JobStatuses[i].Started, returnedBatches[0].JobStatuses[i].Started)
				assert.Equal(t, radixBatch.JobStatuses[i].Ended, returnedBatches[0].JobStatuses[i].Ended)
				assert.Equal(t, radixBatch.JobStatuses[i].Status, returnedBatches[0].JobStatuses[i].Status)
				assert.Equal(t, radixBatch.JobStatuses[i].Message, returnedBatches[0].JobStatuses[i].Message)
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
	batchName := "some-batch-name"
	t.Run("Get batch - success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockHandler(ctrl)
		now := time.Now()
		radixBatch := modelsv2.RadixBatch{
			Name:         batchName,
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
		batchHandler.
			EXPECT().
			GetRadixBatch(batchName).
			Return(&radixBatch, nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("api/v2/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.RadixBatch
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, radixBatch.Name, returnedBatch.Name)
			assert.Equal(t, radixBatch.CreationTime, returnedBatch.CreationTime)
			assert.Equal(t, radixBatch.Started, returnedBatch.Started)
			assert.Equal(t, radixBatch.Ended, returnedBatch.Ended)
			assert.Equal(t, radixBatch.Status, returnedBatch.Status)
			assert.Equal(t, radixBatch.Message, returnedBatch.Message)
			assert.Equal(t, len(radixBatch.JobStatuses), len(returnedBatch.JobStatuses))
			for i := 0; i < len(radixBatch.JobStatuses); i++ {
				assert.Equal(t, radixBatch.JobStatuses[i].Name, returnedBatch.JobStatuses[i].Name)
				assert.Equal(t, radixBatch.JobStatuses[i].CreationTime, returnedBatch.JobStatuses[i].CreationTime)
				assert.Equal(t, radixBatch.JobStatuses[i].Started, returnedBatch.JobStatuses[i].Started)
				assert.Equal(t, radixBatch.JobStatuses[i].Ended, returnedBatch.JobStatuses[i].Ended)
				assert.Equal(t, radixBatch.JobStatuses[i].Status, returnedBatch.JobStatuses[i].Status)
				assert.Equal(t, radixBatch.JobStatuses[i].Message, returnedBatch.JobStatuses[i].Message)
			}
		}
	})
	t.Run("Get batch - status code 500", func(t *testing.T) {
		t.Parallel()
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

/*

func TestCreateBatch(t *testing.T) {
	t.Run("empty body - successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := modelsv2.BatchScheduleDescription{}
		createdBatch := modelsv2.BatchStatus{
			JobStatus: modelsv2.JobStatus{
				Name:    "newbatch",
				Started: commonUtils.FormatTimestamp(time.Now()),
				Ended:   commonUtils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
				Status:  "batchstatus",
			},
		}
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateBatch(&batchScheduleDescription).
			Return(&createdBatch, nil).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			Times(1)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/batches", nil)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.BatchStatus
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, createdBatch.Name, returnedBatch.Name)
			assert.Equal(t, createdBatch.Started, returnedBatch.Started)
			assert.Equal(t, createdBatch.Ended, returnedBatch.Ended)
			assert.Equal(t, createdBatch.Status, returnedBatch.Status)
		}
	})

	t.Run("valid payload body - successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := modelsv2.BatchScheduleDescription{
			JobScheduleDescriptions: []modelsv2.JobScheduleDescription{
				{
					Payload: "a_payload",
					RadixJobComponentConfig: modelsv2.RadixJobComponentConfig{
						Resources: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    "20m",
								"memory": "256M",
							},
							Limits: v1.ResourceList{
								"cpu":    "10m",
								"memory": "128M",
							},
						},
						Node: &v1.RadixNode{
							Gpu:      "nvidia",
							GpuCount: "6",
						},
					},
				},
			},
		}
		createdBatch := modelsv2.BatchStatus{
			JobStatus: modelsv2.JobStatus{
				Name:    "newbatch",
				Started: commonUtils.FormatTimestamp(time.Now()),
				Ended:   commonUtils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
				Status:  "batchstatus",
			},
		}
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateBatch(&batchScheduleDescription).
			Return(&createdBatch, nil).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			Times(1)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/batches", batchScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.BatchStatus
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, createdBatch.Name, returnedBatch.Name)
			assert.Equal(t, createdBatch.Started, returnedBatch.Started)
			assert.Equal(t, createdBatch.Ended, returnedBatch.Ended)
			assert.Equal(t, createdBatch.Status, returnedBatch.Status)
		}
	})

	t.Run("valid payload body - error from MaintainHistoryLimit should not fail request", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := modelsv2.BatchScheduleDescription{
			JobScheduleDescriptions: []modelsv2.JobScheduleDescription{
				{Payload: "a_payload"},
			},
		}
		createdBatch := modelsv2.BatchStatus{
			JobStatus: modelsv2.JobStatus{
				Name:    "newbatch",
				Started: commonUtils.FormatTimestamp(time.Now()),
				Ended:   commonUtils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
				Status:  "batchstatus",
			},
		}
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateBatch(&batchScheduleDescription).
			Return(&createdBatch, nil).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(errors.NewV2("an error")).
			Times(1)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/batches", batchScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch modelsv2.BatchStatus
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, createdBatch.Name, returnedBatch.Name)
			assert.Equal(t, createdBatch.Started, returnedBatch.Started)
			assert.Equal(t, createdBatch.Ended, returnedBatch.Ended)
			assert.Equal(t, createdBatch.Status, returnedBatch.Status)
		}
	})

	t.Run("invalid request body - unprocessable", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateBatch(gomock.Any()).
			Times(0)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Times(0)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/batches", struct{ JobScheduleDescriptions interface{} }{JobScheduleDescriptions: struct{}{}})
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusUnprocessableEntity, response.StatusCode)
			var returnedStatus modelsv2.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusUnprocessableEntity, returnedStatus.Code)
			assert.Equal(t, modelsv2.StatusFailure, returnedStatus.Status)
			assert.Equal(t, modelsv2.StatusReasonInvalid, returnedStatus.Reason)
			assert.Equal(t, apiErrors.InvalidMessage("BatchScheduleDescription"), returnedStatus.Message)
		}
	})

	t.Run("handler returning NotFound error - 404 not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := modelsv2.BatchScheduleDescription{}
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		anyKind, anyName := "anyKind", "anyName"
		batchHandler.
			EXPECT().
			CreateBatch(&batchScheduleDescription).
			Return(nil, apiErrors.NewNotFound(anyKind, anyName)).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Times(0)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodPost, "/api/v1/batches")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusNotFound, response.StatusCode)
			var returnedStatus modelsv2.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, modelsv2.StatusFailure, returnedStatus.Status)
			assert.Equal(t, modelsv2.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage(anyKind, anyName), returnedStatus.Message)
		}
	})

	t.Run("handler returning unhandled error - 500 internal server error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := modelsv2.BatchScheduleDescription{}
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			CreateBatch(&batchScheduleDescription).
			Return(nil, errors.NewV2("any error")).
			Times(1)
		batchHandler.
			EXPECT().
			MaintainHistoryLimit().
			Times(0)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodPost, "/api/v1/batches")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusInternalServerError, response.StatusCode)
			var returnedStatus modelsv2.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, modelsv2.StatusFailure, returnedStatus.Status)
			assert.Equal(t, modelsv2.StatusReasonUnknown, returnedStatus.Reason)
		}
	})
}

func TestDeleteBatch(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchName := "anybatch"
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			DeleteBatch(batchName).
			Return(nil).
			Times(1)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("/api/v1/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedStatus modelsv2.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusOK, returnedStatus.Code)
			assert.Equal(t, modelsv2.StatusSuccess, returnedStatus.Status)
			assert.Empty(t, returnedStatus.Reason)
		}
	})

	t.Run("handler returning not found - 404 not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchName := "anybatch"
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			DeleteBatch(batchName).
			Return(apiErrors.NewNotFound("batch", batchName)).
			Times(1)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("/api/v1/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusNotFound, response.StatusCode)
			var returnedStatus modelsv2.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, modelsv2.StatusFailure, returnedStatus.Status)
			assert.Equal(t, modelsv2.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage("batch", batchName), returnedStatus.Message)
		}
	})

	t.Run("handler returning unhandled error - 500 internal server error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchName := "anybatch"
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			DeleteBatch(batchName).
			Return(errors.NewV2("any error")).
			Times(1)
		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("/api/v1/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusInternalServerError, response.StatusCode)
			var returnedStatus modelsv2.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, modelsv2.StatusFailure, returnedStatus.Status)
			assert.Equal(t, modelsv2.StatusReasonUnknown, returnedStatus.Reason)
		}
	})
}
*/
