package job

import (
	"context"
	"fmt"
	jobDefaults "github.com/equinor/radix-job-scheduler/defaults"
	"sort"
	"strings"
	"time"

	jobErrors "github.com/equinor/radix-job-scheduler-server/api/errors"
	jobKube "github.com/equinor/radix-job-scheduler/kube"
	"github.com/equinor/radix-job-scheduler/models"
	"github.com/equinor/radix-operator/pkg/apis/deployment"
	"github.com/equinor/radix-operator/pkg/apis/kube"
	radixv1 "github.com/equinor/radix-operator/pkg/apis/radix/v1"
	"github.com/equinor/radix-operator/pkg/apis/utils"
	radixclient "github.com/equinor/radix-operator/pkg/client/clientset/versioned"
	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	jobPayloadPropertyName = "payload"
)

type Handler interface {
	//GetJobs Get status of all jobs
	GetJobs() ([]models.JobStatus, error)
	//GetJob Get status of a job
	GetJob(name string) (*models.JobStatus, error)
	//CreateJob Create a job with parameters
	CreateJob(jobScheduleDescription *models.JobScheduleDescription) (*models.JobStatus, error)
	//MaintainHistoryLimit Delete outdated jobs
	MaintainHistoryLimit() error
	//DeleteJob Delete a job
	DeleteJob(jobName string) error
}

type jobHandler struct {
	model *jobKube.HandlerModel
}

func New(env *models.Env, kube *kube.Kube, kubeClient kubernetes.Interface, radixClient radixclient.Interface) Handler {
	return &jobHandler{
		model: &jobKube.HandlerModel{
			Kube:                   kube,
			KubeClient:             kubeClient,
			RadixClient:            radixClient,
			Env:                    env,
			SecurityContextBuilder: deployment.NewSecurityContextBuilder(true),
		},
	}
}

//GetJobs Get status of all jobs
func (jh *jobHandler) GetJobs() ([]models.JobStatus, error) {
	log.Debugf("Get Jobs for namespace: %s", jh.model.Env.RadixDeploymentNamespace)

	kubeJobs, err := jh.getAllJobs()
	if err != nil {
		return nil, err
	}

	pods, err := jh.getJobPods("")
	if err != nil {
		return nil, err
	}
	podsMap := getJobPodsMap(pods)
	jobs := make([]models.JobStatus, len(kubeJobs))
	for idx, k8sJob := range kubeJobs {
		jobs[idx] = *models.GetJobStatusFromJob(jh.model.KubeClient, k8sJob, podsMap[k8sJob.Name])
	}

	log.Debugf("Found %v jobs for namespace %s", len(jobs), jh.model.Env.RadixDeploymentNamespace)
	return jobs, nil
}

func getJobPodsMap(pods []corev1.Pod) map[string][]corev1.Pod {
	podsMap := make(map[string][]corev1.Pod)
	for _, pod := range pods {
		jobName := pod.Labels[jobDefaults.K8sJobNameLabel]
		if len(jobName) > 0 {
			podsMap[jobName] = append(podsMap[jobName], pod)
		}
	}
	return podsMap
}

//GetJob Get status of a job
func (jh *jobHandler) GetJob(jobName string) (*models.JobStatus, error) {
	log.Debugf("get jobs for namespace: %s", jh.model.Env.RadixDeploymentNamespace)
	job, err := jh.getJobByName(jobName)
	if err != nil {
		return nil, err
	}
	log.Debugf("found Job %s for namespace: %s", jobName, jh.model.Env.RadixDeploymentNamespace)
	pods, err := jh.getJobPods(job.Name)
	if err != nil {
		return nil, err
	}
	jobStatus := models.GetJobStatusFromJob(jh.model.KubeClient, job, pods)
	return jobStatus, nil
}

//CreateJob Create a job with parameters
func (jh *jobHandler) CreateJob(jobScheduleDescription *models.JobScheduleDescription) (*models.JobStatus, error) {
	log.Debugf("create job for namespace: %s", jh.model.Env.RadixDeploymentNamespace)

	radixDeployment, err := jh.model.RadixClient.RadixV1().RadixDeployments(jh.model.Env.RadixDeploymentNamespace).Get(context.TODO(), jh.model.Env.RadixDeploymentName, metav1.GetOptions{})
	if err != nil {
		return nil, jobErrors.NewNotFound("radix deployment", jh.model.Env.RadixDeploymentName)
	}

	jobComponent := radixDeployment.GetJobComponentByName(jh.model.Env.RadixComponentName)
	if jobComponent == nil {
		return nil, jobErrors.NewNotFound("job component", jh.model.Env.RadixComponentName)
	}

	jobName := generateJobName(jobComponent)

	payloadSecret, err := jh.model.CreatePayloadSecret(jobName, jobComponent, radixDeployment, jobScheduleDescription)
	if err != nil {
		return nil, jobErrors.NewFromError(err)
	}

	if err = jh.model.CreateService(jobName, jobComponent, radixDeployment); err != nil {
		return nil, jobErrors.NewFromError(err)
	}

	job, err := jh.createJob(jobName, jobComponent, radixDeployment, payloadSecret, jobScheduleDescription)
	if err != nil {
		return nil, jobErrors.NewFromError(err)
	}

	log.Debug(fmt.Sprintf("created job %s for component %s, environment %s, in namespace: %s", job.Name, jh.model.Env.RadixComponentName, radixDeployment.Spec.Environment, jh.model.Env.RadixDeploymentNamespace))
	return models.GetJobStatusFromJob(jh.model.KubeClient, job, nil), nil
}

//DeleteJob Delete a job
func (jh *jobHandler) DeleteJob(jobName string) error {
	log.Debugf("delete job %s for namespace: %s", jobName, jh.model.Env.RadixDeploymentNamespace)
	return jh.garbageCollectJob(jobName)
}

//MaintainHistoryLimit Delete outdated jobs
func (jh *jobHandler) MaintainHistoryLimit() error {
	jobList, err := jh.getAllJobs()
	if err != nil {
		return err
	}

	log.Debug("maintain history limit for succeeded jobs")
	succeededJobs := jobList.Where(func(j *batchv1.Job) bool { return j.Status.Succeeded > 0 })
	if err = jh.maintainHistoryLimitForJobs(succeededJobs, jh.model.Env.RadixJobSchedulersPerEnvironmentHistoryLimit); err != nil {
		return err
	}

	log.Debug("maintain history limit for failed jobs")
	failedJobs := jobList.Where(func(j *batchv1.Job) bool { return j.Status.Failed > 0 })
	if err = jh.maintainHistoryLimitForJobs(failedJobs, jh.model.Env.RadixJobSchedulersPerEnvironmentHistoryLimit); err != nil {
		return err
	}

	return nil
}

func (jh *jobHandler) maintainHistoryLimitForJobs(jobs []*batchv1.Job, historyLimit int) error {
	numToDelete := len(jobs) - historyLimit
	if numToDelete <= 0 {
		log.Debug("no history jobs to delete")
		return nil
	}
	log.Debugf("history jobs to delete: %v", numToDelete)

	sortedJobs := sortRJSchByCompletionTimeAsc(jobs)
	for i := 0; i < numToDelete; i++ {
		job := sortedJobs[i]
		log.Debugf("deleting job %s", job.Name)
		if err := jh.garbageCollectJob(job.Name); err != nil {
			return err
		}
	}
	return nil
}

func (jh *jobHandler) garbageCollectJob(jobName string) (err error) {
	job, err := jh.getJobByName(jobName)
	if err != nil {
		return
	}

	secrets, err := jh.model.GetSecretsForJob(jobName)
	if err != nil {
		return
	}

	for _, secret := range secrets.Items {
		if err = jh.model.DeleteSecret(&secret); err != nil {
			return
		}
	}

	services, err := jh.model.GetServiceForJob(jobName)
	if err != nil {
		return
	}

	for _, service := range services.Items {
		if err = jh.model.DeleteService(&service); err != nil {
			return
		}
	}

	err = jh.deleteJob(job)
	if err != nil {
		return err
	}

	return
}

func sortRJSchByCompletionTimeAsc(jobs []*batchv1.Job) []*batchv1.Job {
	sort.Slice(jobs, func(i, j int) bool {
		job1 := (jobs)[i]
		job2 := (jobs)[j]
		return isRJS1CompletedBeforeRJS2(job1, job2)
	})
	return jobs
}

func isRJS1CompletedBeforeRJS2(job1 *batchv1.Job, job2 *batchv1.Job) bool {
	rd1ActiveFrom := getCompletionTimeFrom(job1)
	rd2ActiveFrom := getCompletionTimeFrom(job2)

	return rd1ActiveFrom.Before(rd2ActiveFrom)
}

func getCompletionTimeFrom(job *batchv1.Job) *metav1.Time {
	if job.Status.CompletionTime.IsZero() {
		return &job.CreationTimestamp
	}
	return job.Status.CompletionTime
}

func generateJobName(jobComponent *radixv1.RadixDeployJobComponent) string {
	timestamp := time.Now().Format("20060102150405")
	jobTag := strings.ToLower(utils.RandString(8))
	return fmt.Sprintf("%s-%s-%s", jobComponent.Name, timestamp, jobTag)
}
