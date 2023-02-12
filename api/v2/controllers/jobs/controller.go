package jobs

import (
	"encoding/json"
	"fmt"
	"github.com/equinor/radix-job-scheduler-server/api/v1/controllers"
	"io"
	"net/http"

	"github.com/equinor/radix-job-scheduler-server/models"
	"github.com/equinor/radix-job-scheduler-server/utils"
	apiErrors "github.com/equinor/radix-job-scheduler/api/errors"
	api "github.com/equinor/radix-job-scheduler/api/v2"
	apiModels "github.com/equinor/radix-job-scheduler/models"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const jobNameParam = "jobName"

type jobController struct {
	*controllers.ControllerBase
	handler api.Handler
}

// New create a new job controller
func New(handler api.Handler) models.Controller {
	return &jobController{
		handler: handler,
	}
}

// GetRoutes List the supported routes of this controller
func (controller *jobController) GetRoutes() models.Routes {
	routes := models.Routes{
		models.Route{
			Path:        "/v2/jobs",
			Method:      http.MethodPost,
			HandlerFunc: controller.CreateJob,
		},
		models.Route{
			Path:        "/v2/jobs",
			Method:      http.MethodGet,
			HandlerFunc: controller.GetJobs,
		},
		models.Route{
			Path:        fmt.Sprintf("/v2/jobs/{%s}", jobNameParam),
			Method:      http.MethodGet,
			HandlerFunc: controller.GetJob,
		},
		models.Route{
			Path:        fmt.Sprintf("/v2/jobs/{%s}", jobNameParam),
			Method:      http.MethodDelete,
			HandlerFunc: controller.DeleteJob,
		},
		models.Route{
			Path:        fmt.Sprintf("/v2/jobs/{%s}/stop", jobNameParam),
			Method:      http.MethodPost,
			HandlerFunc: controller.StopJob,
		},
	}
	return routes
}

// swagger:operation POST /v2/jobs JobV2 createJobV2hack
// ---
// summary: Create job
// parameters:
// - name: jobCreation
//   in: body
//   description: Job to create
//   required: true
//   schema:
//       "$ref": "#/definitions/JobScheduleDescription"
// responses:
//   "200":
//     description: "Successful create job"
//     schema:
//        "$ref": "#/definitions/RadixBatch"
//   "400":
//     description: "Bad request"
//     schema:
//        "$ref": "#/definitions/Status"
//   "404":
//     description: "Not found"
//     schema:
//        "$ref": "#/definitions/Status"
//   "422":
//     description: "Invalid data in request"
//     schema:
//        "$ref": "#/definitions/Status"
//   "500":
//     description: "Internal server error"
//     schema:
//        "$ref": "#/definitions/Status"
func (controller *jobController) CreateJob(w http.ResponseWriter, r *http.Request) {
	var jobScheduleDescription apiModels.JobScheduleDescription

	if body, _ := io.ReadAll(r.Body); len(body) > 0 {
		if err := json.Unmarshal(body, &jobScheduleDescription); err != nil {
			controller.HandleError(w, apiErrors.NewInvalid("payload"))
			return
		}
	}

	jobState, err := controller.handler.CreateRadixBatchSingleJob(&jobScheduleDescription)
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	err = controller.handler.MaintainHistoryLimit()
	if err != nil {
		log.Warnf("failed to maintain job history: %v", err)
	}

	utils.JSONResponse(w, &jobState)
}

// swagger:operation GET /v2/jobs/ JobV2 getJobsV2hack
// ---
// summary: Gets jobs
// parameters:
// responses:
//   "200":
//     description: "Successful get jobs"
//     schema:
//        type: "array"
//        items:
//           "$ref": "#/definitions/RadixBatch"
//   "500":
//     description: "Internal server error"
//     schema:
//        "$ref": "#/definitions/Status"
func (controller *jobController) GetJobs(w http.ResponseWriter, r *http.Request) {
	log.Debug("Get job list")
	jobs, err := controller.handler.GetRadixBatchSingleJobs()
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	log.Debugf("Found %d jobs", len(jobs))
	utils.JSONResponse(w, jobs)
}

// swagger:operation GET /v2/jobs/{jobName} JobV2 getJobV2hack
// ---
// summary: Gets job
// parameters:
// - name: jobName
//   in: path
//   description: Name of job
//   type: string
//   required: true
// responses:
//   "200":
//     description: "Successful get job"
//     schema:
//        "$ref": "#/definitions/RadixBatch"
//   "404":
//     description: "Not found"
//     schema:
//        "$ref": "#/definitions/Status"
//   "500":
//     description: "Internal server error"
//     schema:
//        "$ref": "#/definitions/Status"
func (controller *jobController) GetJob(w http.ResponseWriter, r *http.Request) {
	jobName := mux.Vars(r)[jobNameParam]
	log.Debugf("Get job %s", jobName)
	job, err := controller.handler.GetRadixBatch(jobName)
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	utils.JSONResponse(w, job)
}

// swagger:operation DELETE /v2/jobs/{jobName} JobV2 deleteJobV2hack
// ---
// summary: Delete job
// parameters:
// - name: jobName
//   in: path
//   description: Name of job
//   type: string
//   required: true
// responses:
//   "200":
//     description: "Successful delete job"
//     schema:
//        "$ref": "#/definitions/Status"
//   "404":
//     description: "Not found"
//     schema:
//        "$ref": "#/definitions/Status"
//   "500":
//     description: "Internal server error"
//     schema:
//        "$ref": "#/definitions/Status"
func (controller *jobController) DeleteJob(w http.ResponseWriter, r *http.Request) {
	jobName := mux.Vars(r)[jobNameParam]
	log.Debugf("Delete job %s", jobName)
	err := controller.handler.DeleteRadixBatch(jobName)
	if err != nil {
		controller.HandleError(w, err)
		return
	}

	status := apiModels.Status{
		Status:  apiModels.StatusSuccess,
		Code:    http.StatusOK,
		Message: fmt.Sprintf("job %s successfully deleted", jobName),
	}
	utils.StatusResponse(w, &status)
}

// swagger:operation POST /v2/jobs/{jobName}/stop JobV2 stopJobV2hack
// ---
// summary: Stop job
// parameters:
// - name: jobName
//   in: path
//   description: Name of job
//   type: string
//   required: true
// responses:
//   "200":
//     description: "Successful delete job"
//     schema:
//        "$ref": "#/definitions/Status"
//   "404":
//     description: "Not found"
//     schema:
//        "$ref": "#/definitions/Status"
//   "500":
//     description: "Internal server error"
//     schema:
//        "$ref": "#/definitions/Status"
func (controller *jobController) StopJob(w http.ResponseWriter, r *http.Request) {
	jobName := mux.Vars(r)[jobNameParam]
	log.Debugf("Stop job %s", jobName)
	controller.HandleError(w, fmt.Errorf("stop job is not supported yet"))
}
