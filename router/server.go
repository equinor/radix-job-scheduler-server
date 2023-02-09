package router

import (
	"fmt"
	"net/http"

	"github.com/equinor/radix-job-scheduler-server/models"
	"github.com/equinor/radix-job-scheduler-server/utils"
	schedulerModels "github.com/equinor/radix-job-scheduler/models"
	"github.com/gorilla/mux"
	"github.com/rakyll/statik/fs"
	"github.com/urfave/negroni/v2"
)

const (
	apiVersionRouteV1 = "/api/v1"
	apiVersionRouteV2 = "/api/v2"
)

// NewServer creates a new Radix job scheduler REST service
func NewServer(env *schedulerModels.Env, apiV1Controllers []models.Controller, apiV2Controllers []models.Controller) http.Handler {
	routerV1 := mux.NewRouter().StrictSlash(true)
	routerV2 := mux.NewRouter().StrictSlash(true)

	if env.UseSwagger {
		initSwagger(routerV1, "")
		initSwagger(routerV1, "v2/")
	}

	initializeAPIServer(routerV1, apiVersionRouteV1, apiV1Controllers)
	initializeAPIServer(routerV2, apiVersionRouteV2, apiV2Controllers)

	serveMux := http.NewServeMux()
	serveMux.Handle(apiVersionRouteV1+"/", routerV1)
	serveMux.Handle(apiVersionRouteV2+"/", routerV2)

	if env.UseSwagger {
		serveMux.Handle("/swaggerui/", negroni.New(negroni.Wrap(routerV1), negroni.Wrap(routerV2)))
	}

	recovery := negroni.NewRecovery()
	recovery.PrintStack = false

	n := negroni.New(recovery)
	n.UseHandler(serveMux)
	return n
}

func initSwagger(router *mux.Router, apiVersion string) {
	statikFS, err := fs.New()
	if err != nil {
		panic(err)
	}

	staticServer := http.FileServer(statikFS)
	prefix := fmt.Sprintf("/swaggerui/%s", apiVersion)
	sh := http.StripPrefix(prefix, staticServer)
	router.PathPrefix(prefix).Handler(sh)
}

func initializeAPIServer(router *mux.Router, apiVersionRoute string, controllers []models.Controller) {
	for _, controller := range controllers {
		for _, route := range controller.GetRoutes() {
			addHandlerRoute(router, apiVersionRoute, route)
		}
	}
}

func addHandlerRoute(router *mux.Router, apiVersionRoute string, route models.Route) {
	path := apiVersionRoute + route.Path
	router.HandleFunc(path,
		utils.NewRadixMiddleware(path, route.Method, route.HandlerFunc).Handle).Methods(route.Method)
}
