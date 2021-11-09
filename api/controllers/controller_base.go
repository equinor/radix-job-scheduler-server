package controllers

import (
	jobErrors "github.com/equinor/radix-job-scheduler-server/api/errors"
	"github.com/equinor/radix-job-scheduler-server/utils"
	"github.com/equinor/radix-job-scheduler/models"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type ControllerBase struct {
}

func (controller *ControllerBase) HandleError(w http.ResponseWriter, err error) {
	var status *models.Status

	switch t := err.(type) {
	case jobErrors.APIStatus:
		status = t.Status()
	default:
		status = jobErrors.NewFromError(err).Status()
	}

	log.Errorf("failed: %v", err)
	utils.StatusResponse(w, status)
}
