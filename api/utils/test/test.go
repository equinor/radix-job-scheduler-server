package test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/equinor/radix-job-scheduler-server/models"
	"github.com/equinor/radix-job-scheduler-server/router"
	schedulerModels "github.com/equinor/radix-job-scheduler/models"
)

type ControllerTestUtils struct {
	controllers []models.Controller
}

func New(controllers ...models.Controller) ControllerTestUtils {
	return ControllerTestUtils{
		controllers: controllers,
	}
}

// ExecuteRequest Helper method to issue a http request
func (ctrl *ControllerTestUtils) ExecuteRequest(method, path string) <-chan *http.Response {
	return ctrl.ExecuteRequestWithBody(method, path, nil)
}

// ExecuteRequestWithBody Helper method to issue a http request with body
func (ctrl *ControllerTestUtils) ExecuteRequestWithBody(method, path string, body interface{}) <-chan *http.Response {
	responseChan := make(chan *http.Response)

	go func() {
		var reader io.Reader

		if body != nil {
			payload, _ := json.Marshal(body)
			reader = bytes.NewReader(payload)
		}

		serverRouter := router.NewServer(schedulerModels.NewEnv(), ctrl.controllers...)
		server := httptest.NewServer(serverRouter)
		defer server.Close()
		serverUrl := buildURLFromServer(server, path)
		request, err := http.NewRequest(method, serverUrl, reader)
		if err != nil {
			panic(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			panic(err)
		}
		responseChan <- response
		close(responseChan)
	}()

	return responseChan
}

// GetResponseBody Gets response payload as type
func GetResponseBody(response *http.Response, target interface{}) error {
	body, _ := io.ReadAll(response.Body)

	return json.Unmarshal(body, target)
}

func buildURLFromServer(server *httptest.Server, path string) string {
	serverUrl, _ := url.Parse(server.URL)
	serverUrl.Path = path
	return serverUrl.String()
}
