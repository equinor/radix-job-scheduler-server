package jobs

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/equinor/radix-common/utils"
	"github.com/equinor/radix-job-scheduler-server/api/utils/test"
	apiErrors "github.com/equinor/radix-job-scheduler/api/errors"
	jobApi "github.com/equinor/radix-job-scheduler/api/v1/jobs"
	jobMock "github.com/equinor/radix-job-scheduler/api/v1/jobs/mock"
	models "github.com/equinor/radix-job-scheduler/models/common"
	modelsV1 "github.com/equinor/radix-job-scheduler/models/v1"
	v1 "github.com/equinor/radix-operator/pkg/apis/radix/v1"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setupTest(handler jobApi.JobHandler) *test.ControllerTestUtils {
	jobController := jobController{handler: handler}
	controllerTestUtils := test.New(&jobController)
	return &controllerTestUtils
}

func TestGetJobs(t *testing.T) {
	t.Run("Get jobs - success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobState := modelsV1.JobStatus{
			Name:    "jobname",
			Started: utils.FormatTimestamp(time.Now()),
			Ended:   utils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
			Status:  "jobstatus",
		}
		jobHandler.
			EXPECT().
			GetJobs().
			Return([]modelsV1.JobStatus{jobState}, nil).
			Times(1)

		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v1/jobs")
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedJobs []modelsV1.JobStatus
			test.GetResponseBody(response, &returnedJobs)
			assert.Len(t, returnedJobs, 1)
			assert.Equal(t, jobState.Name, returnedJobs[0].Name)
			assert.Equal(t, jobState.Started, returnedJobs[0].Started)
			assert.Equal(t, jobState.Ended, returnedJobs[0].Ended)
			assert.Equal(t, jobState.Status, returnedJobs[0].Status)
		}
	})

	t.Run("Get jobs - status code 500", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			GetJobs().
			Return(nil, errors.New("unhandled error")).
			Times(1)

		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, "api/v1/jobs")
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

func TestGetJob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobName := "jobname"
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobState := modelsV1.JobStatus{
			Name:    jobName,
			Started: utils.FormatTimestamp(time.Now()),
			Ended:   utils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
			Status:  "jobstatus",
		}
		jobHandler.
			EXPECT().
			GetJob(jobName).
			Return(&jobState, nil).
			Times(1)

		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s", jobName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedJob modelsV1.JobStatus
			test.GetResponseBody(response, &returnedJob)
			assert.Equal(t, jobState.Name, returnedJob.Name)
			assert.Equal(t, jobState.Started, returnedJob.Started)
			assert.Equal(t, jobState.Ended, returnedJob.Ended)
			assert.Equal(t, jobState.Status, returnedJob.Status)
		}
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobName, kind := "anyjob", "job"
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			GetJob(gomock.Any()).
			Return(nil, apiErrors.NewNotFound(kind, jobName)).
			Times(1)

		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s", jobName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusNotFound, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage(kind, jobName), returnedStatus.Message)
		}
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			GetJob(gomock.Any()).
			Return(nil, errors.New("unhandled error")).
			Times(1)

		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s", "anyjob"))
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

func TestCreateJob(t *testing.T) {
	t.Run("empty body - successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobScheduleDescription := models.JobScheduleDescription{}
		createdJob := modelsV1.JobStatus{
			Name:    "newjob",
			Started: utils.FormatTimestamp(time.Now()),
			Ended:   utils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
			Status:  "jobstatus",
		}
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			CreateJob(&jobScheduleDescription).
			Return(&createdJob, nil).
			Times(1)
		jobHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			Times(1)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/jobs", nil)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedJob modelsV1.JobStatus
			test.GetResponseBody(response, &returnedJob)
			assert.Equal(t, createdJob.Name, returnedJob.Name)
			assert.Equal(t, createdJob.Started, returnedJob.Started)
			assert.Equal(t, createdJob.Ended, returnedJob.Ended)
			assert.Equal(t, createdJob.Status, returnedJob.Status)
		}
	})

	t.Run("valid payload body - successful", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobScheduleDescription := models.JobScheduleDescription{
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
		}
		createdJob := modelsV1.JobStatus{
			Name:    "newjob",
			Started: utils.FormatTimestamp(time.Now()),
			Ended:   utils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
			Status:  "jobstatus",
		}
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			CreateJob(&jobScheduleDescription).
			Return(&createdJob, nil).
			Times(1)
		jobHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(nil).
			Times(1)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/jobs", jobScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedJob modelsV1.JobStatus
			test.GetResponseBody(response, &returnedJob)
			assert.Equal(t, createdJob.Name, returnedJob.Name)
			assert.Equal(t, createdJob.Started, returnedJob.Started)
			assert.Equal(t, createdJob.Ended, returnedJob.Ended)
			assert.Equal(t, createdJob.Status, returnedJob.Status)
		}
	})

	t.Run("valid payload body - error from MaintainHistoryLimit should not fail request", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobScheduleDescription := models.JobScheduleDescription{
			Payload: "a_payload",
		}
		createdJob := modelsV1.JobStatus{
			Name:    "newjob",
			Started: utils.FormatTimestamp(time.Now()),
			Ended:   utils.FormatTimestamp(time.Now().Add(1 * time.Minute)),
			Status:  "jobstatus",
		}
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			CreateJob(&jobScheduleDescription).
			Return(&createdJob, nil).
			Times(1)
		jobHandler.
			EXPECT().
			MaintainHistoryLimit().
			Return(errors.New("an error")).
			Times(1)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/jobs", jobScheduleDescription)
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var returnedJob modelsV1.JobStatus
			test.GetResponseBody(response, &returnedJob)
			assert.Equal(t, createdJob.Name, returnedJob.Name)
			assert.Equal(t, createdJob.Started, returnedJob.Started)
			assert.Equal(t, createdJob.Ended, returnedJob.Ended)
			assert.Equal(t, createdJob.Status, returnedJob.Status)
		}
	})

	t.Run("invalid request body - unprocessable", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			CreateJob(gomock.Any()).
			Times(0)
		jobHandler.
			EXPECT().
			MaintainHistoryLimit().
			Times(0)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequestWithBody(http.MethodPost, "/api/v1/jobs", struct{ Payload interface{} }{Payload: struct{}{}})
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusUnprocessableEntity, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusUnprocessableEntity, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonInvalid, returnedStatus.Reason)
			assert.Equal(t, apiErrors.InvalidMessage("payload", ""), returnedStatus.Message)
		}
	})

	t.Run("handler returning NotFound error - 404 not found", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobScheduleDescription := models.JobScheduleDescription{}
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		anyKind, anyName := "anyKind", "anyName"
		jobHandler.
			EXPECT().
			CreateJob(&jobScheduleDescription).
			Return(nil, apiErrors.NewNotFound(anyKind, anyName)).
			Times(1)
		jobHandler.
			EXPECT().
			MaintainHistoryLimit().
			Times(0)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodPost, "/api/v1/jobs")
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
		jobScheduleDescription := models.JobScheduleDescription{}
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			CreateJob(&jobScheduleDescription).
			Return(nil, errors.New("any error")).
			Times(1)
		jobHandler.
			EXPECT().
			MaintainHistoryLimit().
			Times(0)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodPost, "/api/v1/jobs")
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

func TestDeleteJob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobName := "anyjob"
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			DeleteJob(jobName).
			Return(nil).
			Times(1)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("/api/v1/jobs/%s", jobName))
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
		jobName := "anyjob"
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			DeleteJob(jobName).
			Return(apiErrors.NewNotFound("job", jobName)).
			Times(1)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("/api/v1/jobs/%s", jobName))
		response := <-responseChannel
		assert.NotNil(t, response)

		if response != nil {
			assert.Equal(t, http.StatusNotFound, response.StatusCode)
			var returnedStatus models.Status
			test.GetResponseBody(response, &returnedStatus)
			assert.Equal(t, http.StatusNotFound, returnedStatus.Code)
			assert.Equal(t, models.StatusFailure, returnedStatus.Status)
			assert.Equal(t, models.StatusReasonNotFound, returnedStatus.Reason)
			assert.Equal(t, apiErrors.NotFoundMessage("job", jobName), returnedStatus.Message)
		}
	})

	t.Run("handler returning unhandled error - 500 internal server error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		jobName := "anyjob"
		jobHandler := jobMock.NewMockJobHandler(ctrl)
		jobHandler.
			EXPECT().
			DeleteJob(jobName).
			Return(errors.New("any error")).
			Times(1)
		controllerTestUtils := setupTest(jobHandler)
		responseChannel := controllerTestUtils.ExecuteRequest(http.MethodDelete, fmt.Sprintf("/api/v1/jobs/%s", jobName))
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
