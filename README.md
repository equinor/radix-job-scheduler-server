# Radix job scheduler server

# This project is now archived - 2023.11.13
This repository is not longer in use, code merged into radix-job-scheduler

The job scheduler server for application jobs

## Usage
Request from application container URLs
* `POST` `http://<job-name>:8080/api/v1/jobs` - start new job 
* `GET` `http://<job-name>:8080/api/v1/jobs` - get job list
* `GET` `http://<job-name>:8080/api/v1/jobs/<job-name>` - get job status 
* `DELETE` `http://<job-name>:8080/api/v1/jobs/<job-name>` - stop and delete job 

## Developing

You need Go installed. Make sure `GOPATH` and `GOROOT` are properly set up.

Also needed:

- [`go-swagger`](https://github.com/go-swagger/go-swagger) (on a Mac, you can install it with Homebrew: `brew install go-swagger`)
- [`statik`](https://github.com/rakyll/statik) (install with `go get github.com/rakyll/statik`)

Clone the repo into your `GOPATH` and run `go mod download`.

#### Update version
We follow the [semantic version](https://semver.org/) as recommended by [go](https://blog.golang.org/publishing-go-modules).
`radix-job-scheduler-server` has three places to set version
* `apiVersionRoute` in `router/server.go` and `BasePath`in `docs/docs.go` - API version, used in API's URL
* `Version` in `docs/docs.go` - indicates changes in radix-job-scheduler-server logic - to see (e.g in swagger), that the version in the environment corresponds with what you wanted

  Run following command to update version in `swagger.json`
    ```
    make swagger
    ``` 

* If generated file `swagger.json` is changed (methods or structures) - copy it to the [public site](https://github.com/equinor/radix-public-site/tree/main/public-site/docs/src/guides/configure-jobs) 

### Custom configuration

By default `Info` and `Error` messages are logged. This can be configured via environment variable `LOG_LEVEL` (pods need to be restarted after changes)
* `LOG_LEVEL=ERROR` - log only `Error` messages
* `LOG_LEVEL=INFO` or not set - log `Info` and `Error` messages
* `LOG_LEVEL=WARNING` or not set - log `Info`, `Warning` and `Error` messages
* `LOG_LEVEL=DEBUG` - log `Debug`, `Warning`, `Info` and `Error` messages

By default `swagger UI` is not available. This can be configured via environment variable `USE_SWAGGER`
* `USE_SWAGGER=true` - allows to use swagger UI with URL `<api-endpoint>/swaggerui`
