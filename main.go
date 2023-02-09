package main

import (
	"fmt"
	"net/http"
	"os"

	batchControllersV1 "github.com/equinor/radix-job-scheduler-server/api/v1/controllers/batches"
	jobControllersV1 "github.com/equinor/radix-job-scheduler-server/api/v1/controllers/jobs"
	batchControllersV2 "github.com/equinor/radix-job-scheduler-server/api/v2/controllers/batches"
	jobControllersV2 "github.com/equinor/radix-job-scheduler-server/api/v2/controllers/jobs"
	"github.com/equinor/radix-job-scheduler-server/models"
	"github.com/equinor/radix-job-scheduler-server/router"
	_ "github.com/equinor/radix-job-scheduler-server/swaggerui"
	batchApiV1 "github.com/equinor/radix-job-scheduler/api/v1/batches"
	jobApiV1 "github.com/equinor/radix-job-scheduler/api/v1/jobs"
	batchApiV2 "github.com/equinor/radix-job-scheduler/api/v2"
	apiModels "github.com/equinor/radix-job-scheduler/models"
	"github.com/equinor/radix-operator/pkg/apis/kube"
	"github.com/equinor/radix-operator/pkg/apis/utils"
	"github.com/gorilla/handlers"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

type apiControllers struct {
	v1 []models.Controller
	v2 []models.Controller
}

func main() {
	env := apiModels.NewEnv()
	fs := initializeFlagSet()

	var (
		port = fs.StringP("port", "p", env.RadixPort, "Port where API will be served")
	)

	log.Debugf("Port: %s\n", *port)
	parseFlagsFromArgs(fs)

	errs := make(chan error)

	go func() {
		log.Infof("Radix job scheduler API is serving on port %s", *port)
		apiControllers := getControllers(env)
		err := http.ListenAndServe(fmt.Sprintf(":%s", *port), handlers.CombinedLoggingHandler(os.Stdout, router.NewServer(env, apiControllers.v1, apiControllers.v2)))
		errs <- err
	}()

	err := <-errs
	if err != nil {
		log.Fatalf("Radix job scheduler API server crashed: %v", err)
	}
}

func getControllers(env *apiModels.Env) *apiControllers {
	kubeClient, radixClient, _, secretProviderClient := utils.GetKubernetesClient()
	kubeUtil, _ := kube.New(kubeClient, radixClient, secretProviderClient)
	return &apiControllers{
		v1: []models.Controller{
			jobControllersV1.New(jobApiV1.New(env, kubeUtil)),
			batchControllersV1.New(batchApiV1.New(env, kubeUtil, kubeClient, radixClient)),
		},
		v2: []models.Controller{
			jobControllersV2.New(batchApiV2.New(env, kubeUtil, kubeClient, radixClient)),
			batchControllersV2.New(batchApiV2.New(env, kubeUtil, kubeClient, radixClient)),
		},
	}
}

func initializeFlagSet() *pflag.FlagSet {
	// Flag domain.
	fs := pflag.NewFlagSet("default", pflag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "DESCRIPTION\n")
		fmt.Fprint(os.Stderr, "Radix job scheduler API server.\n")
		fmt.Fprint(os.Stderr, "\n")
		fmt.Fprint(os.Stderr, "FLAGS\n")
		fs.PrintDefaults()
	}
	return fs
}

func parseFlagsFromArgs(fs *pflag.FlagSet) {
	err := fs.Parse(os.Args[1:])
	switch {
	case err == pflag.ErrHelp:
		os.Exit(0)
	case err != nil:
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err.Error())
		fs.Usage()
		os.Exit(2)
	}
}
