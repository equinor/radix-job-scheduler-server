package batch

import (
	"context"
	"fmt"
	jobDefaults "github.com/equinor/radix-job-scheduler/defaults"
	jobKube "github.com/equinor/radix-job-scheduler/kube"
	"sort"
	"strings"
	"time"

	jobErrors "github.com/equinor/radix-job-scheduler-server/api/errors"
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
	batchScheduleDescriptionSecretPropertyName = "batchScheduleDescription"
)

type Handler interface {
	//GetBatches Get status of all batches
	GetBatches() ([]models.BatchStatus, error)
	//GetBatch Get status of a batch
	GetBatch(batchName string) (*models.BatchStatus, error)
	//CreateBatch Create a batch with parameters
	CreateBatch(batchScheduleDescription *models.BatchScheduleDescription) (*models.BatchStatus, error)
	//MaintainHistoryLimit Delete outdated batches
	MaintainHistoryLimit() error
	//DeleteBatch Delete a batch
	DeleteBatch(batchName string) error
}

type batchHandler struct {
	model *jobKube.HandlerModel
}

func New(env *models.Env, kube *kube.Kube, kubeClient kubernetes.Interface, radixClient radixclient.Interface) Handler {
	return &batchHandler{
		model: &jobKube.HandlerModel{
			Kube:                   kube,
			KubeClient:             kubeClient,
			RadixClient:            radixClient,
			Env:                    env,
			SecurityContextBuilder: deployment.NewSecurityContextBuilder(true),
		},
	}
}

//GetBatches Get status of all batches
func (bh *batchHandler) GetBatches() ([]models.BatchStatus, error) {
	log.Debugf("Get Batches for namespace: %s", bh.model.Env.RadixDeploymentNamespace)

	//TODO
	//kubeBatches, err := bh.getAllBatches()
	//if err != nil {
	//    return nil, err
	//}

	//pods, err := bh.GetBatchPods("")
	//if err != nil {
	//    return nil, err
	//}
	//podsMap := getBatchPodsMap(pods)
	//batches := make([]models.BatchStatus, len(kubeBatches))
	//for idx, k8sBatch := range kubeBatches {
	//    batches[idx] = *models.GetBatchStatusFromJob(bh.model.KubeClient, k8sBatch, podsMap[k8sBatch.Name])
	//}

	//log.Debugf("Found %v batches for namespace %s", len(batches), bh.model.Env.RadixDeploymentNamespace)
	//return batches, nil
	return nil, nil
}

func getBatchPodsMap(pods []corev1.Pod) map[string][]corev1.Pod {
	podsMap := make(map[string][]corev1.Pod)
	for _, pod := range pods {
		batchName := pod.Labels[jobDefaults.K8sJobNameLabel]
		if len(batchName) > 0 {
			podsMap[batchName] = append(podsMap[batchName], pod)
		}
	}
	return podsMap
}

//GetBatch Get status of a batch
func (bh *batchHandler) GetBatch(batchName string) (*models.BatchStatus, error) {
	log.Debugf("get batches for namespace: %s", bh.model.Env.RadixDeploymentNamespace)
	batch, err := bh.getBatchByName(batchName)
	if err != nil {
		return nil, err
	}
	log.Debugf("found Batch %s for namespace: %s", batchName, bh.model.Env.RadixDeploymentNamespace)
	pods, err := bh.getJobPods(batch.Name)
	if err != nil {
		return nil, err
	}
	batchStatus := &models.BatchStatus{
		JobStatus:   *models.GetJobStatusFromJob(bh.model.KubeClient, batch, pods),
		JobStatuses: nil, //TODO
	}
	return batchStatus, nil
}

//CreateBatch Create a batch with parameters
func (bh *batchHandler) CreateBatch(batchScheduleDescription *models.BatchScheduleDescription) (*models.BatchStatus, error) {
	log.Debugf("create batch for namespace: %s", bh.model.Env.RadixDeploymentNamespace)

	radixDeployment, err := bh.model.RadixClient.RadixV1().RadixDeployments(bh.model.Env.RadixDeploymentNamespace).Get(context.TODO(), bh.model.Env.RadixDeploymentName, metav1.GetOptions{})
	if err != nil {
		return nil, jobErrors.NewNotFound("radix deployment", bh.model.Env.RadixDeploymentName)
	}

	jobComponent := radixDeployment.GetJobComponentByName(bh.model.Env.RadixComponentName)
	if jobComponent == nil {
		return nil, jobErrors.NewNotFound("job component", bh.model.Env.RadixComponentName)
	}

	batchName := generateBatchName(jobComponent)

	if err = bh.model.CreateService(batchName, jobComponent, radixDeployment); err != nil {
		return nil, jobErrors.NewFromError(err)
	}

	batch, err := bh.createBatch(batchName, jobComponent, radixDeployment, payloadSecret, batchScheduleDescription)
	if err != nil {
		return nil, jobErrors.NewFromError(err)
	}

	log.Debug(fmt.Sprintf("created batch %s for component %s, environment %s, in namespace: %s", batch.Name, bh.model.Env.RadixComponentName, radixDeployment.Spec.Environment, bh.model.Env.RadixDeploymentNamespace))
	return models.GetBatchStatusFromBatch(bh.model.KubeClient, batch, nil), nil
}

//DeleteBatch Delete a batch
func (bh *batchHandler) DeleteBatch(batchName string) error {
	log.Debugf("delete batch %s for namespace: %s", batchName, bh.model.Env.RadixDeploymentNamespace)
	return bh.garbageCollectBatch(batchName)
}

//MaintainHistoryLimit Delete outdated batches
func (bh *batchHandler) MaintainHistoryLimit() error {
	batchList, err := bh.getAllBatches()
	if err != nil {
		return err
	}

	log.Debug("maintain history limit for succeeded batches")
	succeededBatches := batchList.Where(func(j *batchv1.Batch) bool { return j.Status.Succeeded > 0 })
	if err = bh.maintainHistoryLimitForBatches(succeededBatches, bh.model.Env.RadixBatchSchedulersPerEnvironmentHistoryLimit); err != nil {
		return err
	}

	log.Debug("maintain history limit for failed batches")
	failedBatches := batchList.Where(func(j *batchv1.Batch) bool { return j.Status.Failed > 0 })
	if err = bh.maintainHistoryLimitForBatches(failedBatches, bh.model.Env.RadixBatchSchedulersPerEnvironmentHistoryLimit); err != nil {
		return err
	}

	return nil
}

func (bh *batchHandler) maintainHistoryLimitForBatches(batches []*batchv1.Batch, historyLimit int) error {
	numToDelete := len(batches) - historyLimit
	if numToDelete <= 0 {
		log.Debug("no history batches to delete")
		return nil
	}
	log.Debugf("history batches to delete: %v", numToDelete)

	sortedBatches := sortRJSchByCompletionTimeAsc(batches)
	for i := 0; i < numToDelete; i++ {
		batch := sortedBatches[i]
		log.Debugf("deleting batch %s", batch.Name)
		if err := bh.garbageCollectBatch(batch.Name); err != nil {
			return err
		}
	}
	return nil
}

func (bh *batchHandler) garbageCollectBatch(batchName string) (err error) {
	batch, err := bh.getBatchByName(batchName)
	if err != nil {
		return
	}

	secrets, err := bh.getSecretsForBatch(batchName)
	if err != nil {
		return
	}

	for _, secret := range secrets.Items {
		if err = bh.deleteSecret(&secret); err != nil {
			return
		}
	}

	services, err := bh.getServiceForBatch(batchName)
	if err != nil {
		return
	}

	for _, service := range services.Items {
		if err = bh.deleteService(&service); err != nil {
			return
		}
	}

	err = bh.deleteBatch(batch)
	if err != nil {
		return err
	}

	return
}

func sortRJSchByCompletionTimeAsc(batches []*batchv1.Batch) []*batchv1.Batch {
	sort.Slice(batches, func(i, j int) bool {
		batch1 := (batches)[i]
		batch2 := (batches)[j]
		return isRJS1CompletedBeforeRJS2(batch1, batch2)
	})
	return batches
}

func isRJS1CompletedBeforeRJS2(batch1 *batchv1.Batch, batch2 *batchv1.Batch) bool {
	rd1ActiveFrom := getCompletionTimeFrom(batch1)
	rd2ActiveFrom := getCompletionTimeFrom(batch2)

	return rd1ActiveFrom.Before(rd2ActiveFrom)
}

func getCompletionTimeFrom(batch *batchv1.Job) *metav1.Time {
	if batch.Status.CompletionTime.IsZero() {
		return &batch.CreationTimestamp
	}
	return batch.Status.CompletionTime
}

func generateBatchName(jobComponent *radixv1.RadixDeployJobComponent) string {
	timestamp := time.Now().Format("20060102150405")
	jobTag := strings.ToLower(utils.RandString(8))
	return fmt.Sprintf("batch-%s-%s-%s", jobComponent.Name, timestamp, jobTag)
}

func isPayloadDefinedForJobComponent(radixJobComponent *radixv1.RadixDeployJobComponent) bool {
	return radixJobComponent.Payload != nil && strings.TrimSpace(radixJobComponent.Payload.Path) != ""
}
