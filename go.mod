module github.com/equinor/radix-job-scheduler-server

go 1.16

require (
	github.com/equinor/radix-common v1.1.6
	github.com/equinor/radix-job-scheduler v1.3.0
	github.com/equinor/radix-operator v1.16.8
	github.com/golang/mock v1.5.0
	github.com/gorilla/mux v1.8.0
	github.com/rakyll/statik v0.1.6
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/urfave/negroni v1.0.0
	k8s.io/api v0.22.4
	k8s.io/apimachinery v0.22.4
	k8s.io/client-go v12.0.0+incompatible
)

replace (
	//github.com/equinor/radix-operator => /home/user1/go/src/github.com/equinor/radix-operator
	k8s.io/client-go => k8s.io/client-go v0.22.4
)
