package controllers

import (
	"net/http"

	"github.com/equinor/radix-job-scheduler-server/utils"
	apiErrors "github.com/equinor/radix-job-scheduler/api/errors"
	models "github.com/equinor/radix-job-scheduler/models/common"
	log "github.com/sirupsen/logrus"
)

type ControllerBase struct {
}

func (controller *ControllerBase) HandleError(w http.ResponseWriter, err error) {
	var status *models.Status

	switch t := err.(type) {
	case apiErrors.APIStatus:
		status = t.Status()
	default:
		status = apiErrors.NewFromError(err).Status()
	}

	log.Errorf("failed: %v", err)
	utils.StatusResponse(w, status)
}
