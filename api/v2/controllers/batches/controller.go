package batch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/equinor/radix-job-scheduler-server/api/controllers"
	"github.com/equinor/radix-job-scheduler-server/models"
	"github.com/equinor/radix-job-scheduler-server/utils"
	apiErrors "github.com/equinor/radix-job-scheduler/api/errors"
	radixBatchApi "github.com/equinor/radix-job-scheduler/api/v2"
	schedulerModels "github.com/equinor/radix-job-scheduler/models"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const (
	batchNameParam = "batchName"
	jobNameParam   = "jobName"
)

type batchController struct {
	*controllers.ControllerBase
	handler radixBatchApi.Handler
}

// New create a new batch controller
func New(handler radixBatchApi.Handler) models.Controller {
	return &batchController{
		handler: handler,
	}
}

// GetRoutes List the supported routes of this controller
func (controller *batchController) GetRoutes() models.Routes {
	routes := models.Routes{
		models.Route{
			Path:        "/v2/batches",
			Method:      http.MethodPost,
			HandlerFunc: controller.CreateBatch,
		},
		models.Route{
			Path:        "/v2/batches",
			Method:      http.MethodGet,
			HandlerFunc: controller.GetBatches,
		},
		models.Route{
			Path:        fmt.Sprintf("/v2/batches/{%s}", batchNameParam),
			Method:      http.MethodGet,
			HandlerFunc: controller.GetBatch,
		},
		models.Route{
			Path:        fmt.Sprintf("/v2/batches/{%s}", batchNameParam),
			Method:      http.MethodDelete,
			HandlerFunc: controller.DeleteBatch,
		},
		models.Route{
			Path:        fmt.Sprintf("/v2/batches/{%s}/stop", batchNameParam),
			Method:      http.MethodPost,
			HandlerFunc: controller.StopBatch,
		},
		models.Route{
			Path:        fmt.Sprintf("/v2/batches/{%s}/jobs/{%s}/stop", batchNameParam, jobNameParam),
			Method:      http.MethodPost,
			HandlerFunc: controller.StopBatchJob,
		},
	}
	return routes
}

// swagger:operation POST /v2/batches BatchV2 createBatchV2hack
// ---
// summary: Create batch
// parameters:
// - name: batchCreation
//   in: body
//   description: BatchV2 to create
//   required: true
//   schema:
//       "$ref": "#/definitions/BatchScheduleDescription"
// responses:
//   "200":
//     description: "Successful create batch"
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
func (controller *batchController) CreateBatch(w http.ResponseWriter, r *http.Request) {
	var batchScheduleDescription schedulerModels.BatchScheduleDescription

	if body, _ := io.ReadAll(r.Body); len(body) > 0 {
		if err := json.Unmarshal(body, &batchScheduleDescription); err != nil {
			controller.HandleError(w, apiErrors.NewInvalid("BatchScheduleDescription"))
			return
		}
	}

	radixBatch, err := controller.handler.CreateRadixBatch(&batchScheduleDescription)
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	err = controller.handler.MaintainHistoryLimit()
	if err != nil {
		log.Warnf("failed to maintain batch history: %v", err)
	}

	utils.JSONResponse(w, &radixBatch)
}

// swagger:operation GET /v2/batches/ BatchV2 getBatchesV2hack
// ---
// summary: Gets batches
// parameters:
// responses:
//   "200":
//     description: "Successful get batches"
//     schema:
//        type: "array"
//        items:
//           "$ref": "#/definitions/RadixBatch"
//   "500":
//     description: "Internal server error"
//     schema:
//        "$ref": "#/definitions/Status"
func (controller *batchController) GetBatches(w http.ResponseWriter, r *http.Request) {
	log.Debug("Get batch list")
	batches, err := controller.handler.GetRadixBatches()
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	log.Debugf("Found %d batches", len(batches))
	utils.JSONResponse(w, batches)
}

// swagger:operation GET /v2/batches/{batchName} BatchV2 getBatchV2hack
// ---
// summary: Gets batch
// parameters:
// - name: batchName
//   in: path
//   description: Name of batch
//   type: string
//   required: true
// responses:
//   "200":
//     description: "Successful get batch"
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
func (controller *batchController) GetBatch(w http.ResponseWriter, r *http.Request) {
	batchName := mux.Vars(r)[batchNameParam]
	log.Debugf("Get batch %s", batchName)
	radixBatch, err := controller.handler.GetRadixBatch(batchName)
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	utils.JSONResponse(w, radixBatch)
}

// swagger:operation DELETE /v2/batches/{batchName} BatchV2 deleteBatchV2hack
// ---
// summary: Delete batch
// parameters:
// - name: batchName
//   in: path
//   description: Name of batch
//   type: string
//   required: true
// responses:
//   "200":
//     description: "Successful delete batch"
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
func (controller *batchController) DeleteBatch(w http.ResponseWriter, r *http.Request) {
	batchName := mux.Vars(r)[batchNameParam]
	log.Debugf("Delete batch %s", batchName)
	err := controller.handler.DeleteRadixBatch(batchName)
	if err != nil {
		controller.HandleError(w, err)
		return
	}

	status := schedulerModels.Status{
		Status:  schedulerModels.StatusSuccess,
		Code:    http.StatusOK,
		Message: fmt.Sprintf("batch %s successfully deleted", batchName),
	}
	utils.StatusResponse(w, &status)
}

// swagger:operation POST /v2/batches/{batchName}/stop BatchV2 stopBatchV2hack
// ---
// summary: Stop batch
// parameters:
// - name: batchName
//   in: path
//   description: Name of batch
//   type: string
//   required: true
// responses:
//   "200":
//     description: "Successful stop batch"
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
func (controller *batchController) StopBatch(w http.ResponseWriter, r *http.Request) {
	batchName := mux.Vars(r)[batchNameParam]
	log.Debugf("Stop batch %s", batchName)
	controller.HandleError(w, fmt.Errorf("stop batch is not supported yet"))
}

// swagger:operation POST /v2/batches/{batchName}/jobs/{jobName}/stop BatchV2 stopBatchJobV2hack
// ---
// summary: Stop batch job
// parameters:
// - name: batchName
//   in: path
//   description: Name of batch
//   type: string
//   required: true
// - name: jobName
//   in: path
//   description: Name of job
//   type: string
//   required: true
// responses:
//   "200":
//     description: "Successful stop batch job"
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
func (controller *batchController) StopBatchJob(w http.ResponseWriter, r *http.Request) {
	batchName := mux.Vars(r)[batchNameParam]
	log.Debugf("Stop batch job %s", batchName)
	controller.HandleError(w, fmt.Errorf("stop batch job is not supported yet"))
}
