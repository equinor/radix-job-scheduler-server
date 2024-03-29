package main

import (
	"fmt"
	"net/http"
	"os"

	batchControllers "github.com/equinor/radix-job-scheduler-server/api/v1/controllers/batches"
	jobControllers "github.com/equinor/radix-job-scheduler-server/api/v1/controllers/jobs"
	"github.com/equinor/radix-job-scheduler-server/models"
	"github.com/equinor/radix-job-scheduler-server/router"
	_ "github.com/equinor/radix-job-scheduler-server/swaggerui"
	batchApi "github.com/equinor/radix-job-scheduler/api/v1/batches"
	jobApi "github.com/equinor/radix-job-scheduler/api/v1/jobs"
	apiModels "github.com/equinor/radix-job-scheduler/models"
	"github.com/equinor/radix-operator/pkg/apis/kube"
	"github.com/equinor/radix-operator/pkg/apis/utils"
	"github.com/gorilla/handlers"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

func main() {
	env := apiModels.NewEnv()
	fs := initializeFlagSet()

	var (
		port = fs.StringP("port", "p", env.RadixPort, "Port where API will be served")
	)

	log.Debugf("Port: %s\n", *port)
	parseFlagsFromArgs(fs)

	errs := make(chan error)
	kubeUtil := getKubeUtil()

	go func() {
		log.Infof("Radix job scheduler API is serving on port %s", *port)
		err := http.ListenAndServe(fmt.Sprintf(":%s", *port), handlers.CombinedLoggingHandler(os.Stdout, router.NewServer(env, getControllers(kubeUtil, env)...)))
		errs <- err
	}()

	err := <-errs
	if err != nil {
		log.Fatalf("Radix job scheduler API server crashed: %v", err)
	}
}

func getKubeUtil() *kube.Kube {
	kubeClient, radixClient, _, secretProviderClient := utils.GetKubernetesClient()
	kubeUtil, _ := kube.New(kubeClient, radixClient, secretProviderClient)
	return kubeUtil
}

func getControllers(kubeUtil *kube.Kube, env *apiModels.Env) []models.Controller {
	return []models.Controller{
		jobControllers.New(jobApi.New(kubeUtil, env)),
		batchControllers.New(batchApi.New(kubeUtil, env)),
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
