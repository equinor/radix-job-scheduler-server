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

const batchNameParam = "batchName"

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
			Path:        "/batches",
			Method:      http.MethodPost,
			HandlerFunc: controller.CreateBatch,
		},
		models.Route{
			Path:        "/batches",
			Method:      http.MethodGet,
			HandlerFunc: controller.GetBatches,
		},
		models.Route{
			Path:        fmt.Sprintf("/batches/{%s}", batchNameParam),
			Method:      http.MethodGet,
			HandlerFunc: controller.GetBatchStatus,
		},
		models.Route{
			Path:        fmt.Sprintf("/batches/{%s}", batchNameParam),
			Method:      http.MethodDelete,
			HandlerFunc: controller.DeleteBatch,
		},
	}
	return routes
}

// swagger:operation POST /batches Batch createBatch
// ---
// summary: Create batch
// parameters:
// - name: batchCreation
//   in: body
//   description: Batch to create
//   required: true
//   schema:
//       "$ref": "#/definitions/BatchScheduleDescription"
// responses:
//   "200":
//     description: "Successful create batch"
//     schema:
//        "$ref": "#/definitions/BatchStatus"
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

	batchState, err := controller.handler.CreateRadixBatch(&batchScheduleDescription)
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	err = controller.handler.MaintainHistoryLimit()
	if err != nil {
		log.Warnf("failed to maintain batch history: %v", err)
	}

	utils.JSONResponse(w, &batchState)
}

// swagger:operation GET /batches/ Batch getBatches
// ---
// summary: Gets batches
// parameters:
// responses:
//   "200":
//     description: "Successful get batches"
//     schema:
//        type: "array"
//        items:
//           "$ref": "#/definitions/BatchStatus"
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

// swagger:operation GET /batches/{batchName} Batch getBatch
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
//        "$ref": "#/definitions/BatchStatus"
//   "404":
//     description: "Not found"
//     schema:
//        "$ref": "#/definitions/Status"
//   "500":
//     description: "Internal server error"
//     schema:
//        "$ref": "#/definitions/Status"
func (controller *batchController) GetBatchStatus(w http.ResponseWriter, r *http.Request) {
	batchName := mux.Vars(r)[batchNameParam]
	log.Debugf("Get batch %s", batchName)
	batch, err := controller.handler.GetRadixBatch(batchName)
	if err != nil {
		controller.HandleError(w, err)
		return
	}
	utils.JSONResponse(w, batch)
}

// swagger:operation DELETE /batches/{batchName} Batch deleteBatch
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
