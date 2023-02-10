package batch

import (
	"github.com/equinor/radix-common/utils/pointers"
	"net/http"
	"testing"
	"time"

	commonUtils "github.com/equinor/radix-common/utils"
	"github.com/equinor/radix-job-scheduler-server/api/utils/test"
	radixBatchApi "github.com/equinor/radix-job-scheduler/api/v2"
	batchMock "github.com/equinor/radix-job-scheduler/api/v2/mock"
	models "github.com/equinor/radix-job-scheduler/models/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func setupTest(handler radixBatchApi.Handler) *test.ControllerTestUtils {
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
		radixBatch := models.RadixBatch{
			Name:         "some-batch-name",
			CreationTime: commonUtils.FormatTimestamp(now),
			Started:      commonUtils.FormatTime(pointers.Ptr(metav1.NewTime(now.Add(time.Minute)))),
			Ended:        commonUtils.FormatTime(pointers.Ptr(metav1.NewTime(now.Add(time.Minute * 50)))),
			Status:       "some status",
			Message:      "test message",
			JobStatuses: []models.RadixBatchJobStatus{
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
			Return([]models.RadixBatch{radixBatch}, nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v2/batches")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatches []models.RadixBatch
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
	/*
		t.Run("Get batches - status code 500", func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			batchHandler := batchMock.NewMockBatchHandler(ctrl)
			batchHandler.
				EXPECT().
				GetBatches().
				Return(nil, errors.NewV2("unhandled error")).
				Times(1)

			controllerTestUtils := setupTest(batchHandler)
			responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v1/batches")
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
	*/
}

/*func TestGetBatch(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchName := "batchname"
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchState := models.BatchStatus{
			JobStatus: models.JobStatus{
				Name:    batchName,
				Started: commonUtils.FormatTimestamp(time.Now()),
				Ended:   commonUtils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
				Status:  "batchstatus",
			},
		}
		batchHandler.
			EXPECT().
			GetBatch(batchName).
			Return(&batchState, nil).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("/api/v1/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedBatch models.BatchStatus
			test.GetResponseBody(response, &returnedBatch)
			assert.Equal(t, batchState.Name, returnedBatch.Name)
			assert.Equal(t, batchState.Started, returnedBatch.Started)
			assert.Equal(t, batchState.Ended, returnedBatch.Ended)
			assert.Equal(t, batchState.Status, returnedBatch.Status)
		}
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchName, kind := "anybatch", "batch"
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			GetBatch(gomock.Any()).
			Return(nil, apiErrors.NewNotFound(kind, batchName)).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("/api/v1/batches/%s", batchName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusNotFound, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage(kind, batchName), returnedStatus.Message)
		}
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchHandler := batchMock.NewMockBatchHandler(ctrl)
		batchHandler.
			EXPECT().
			GetBatch(gomock.Any()).
			Return(nil, errors.NewV2("unhandled error")).
			Times(1)

		controllerTestUtils := setupTest(batchHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("/api/v1/batches/%s", "anybatch"))
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
	t.Run("empty body - successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := models.BatchScheduleDescription{}
		createdBatch := models.BatchStatus{
			JobStatus: models.JobStatus{
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
			var returnedBatch models.BatchStatus
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
		batchScheduleDescription := models.BatchScheduleDescription{
			JobScheduleDescriptions: []models.JobScheduleDescription{
				{
					Payload: "a_payload",
					RadixJobComponentConfig: models.RadixJobComponentConfig{
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
		createdBatch := models.BatchStatus{
			JobStatus: models.JobStatus{
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
			var returnedBatch models.BatchStatus
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
		batchScheduleDescription := models.BatchScheduleDescription{
			JobScheduleDescriptions: []models.JobScheduleDescription{
				{Payload: "a_payload"},
			},
		}
		createdBatch := models.BatchStatus{
			JobStatus: models.JobStatus{
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
			var returnedBatch models.BatchStatus
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
		batchScheduleDescription := models.BatchScheduleDescription{}
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
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage(anyKind, anyName), returnedStatus.Message)
		}
	})

	t.Run("handler returning unhandled error - 500 internal server error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		batchScheduleDescription := models.BatchScheduleDescription{}
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
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonUnknown, returnedStatus.Reason)
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
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusOK, returnedStatus.Code)
			assert.Equal(t, models.StatusSuccess, returnedStatus.Status)
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
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonNotFound, returnedStatus.Reason)
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
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusInternalServerError, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonUnknown, returnedStatus.Reason)
		}
	})
}
*/
