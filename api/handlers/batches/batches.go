package batches

import (
    "context"
    jobDefaults "github.com/equinor/radix-job-scheduler/defaults"

    radixutils "github.com/equinor/radix-common/utils"
    jobErrors "github.com/equinor/radix-job-scheduler-server/api/errors"
    "github.com/equinor/radix-job-scheduler/models"
    "github.com/equinor/radix-operator/pkg/apis/deployment"
    "github.com/equinor/radix-operator/pkg/apis/kube"
    radixv1 "github.com/equinor/radix-operator/pkg/apis/radix/v1"
    operatorUtils "github.com/equinor/radix-operator/pkg/apis/utils"
    "github.com/equinor/radix-operator/pkg/apis/utils/numbers"
    "github.com/equinor/radix-operator/pkg/apis/utils/slice"
    batchv1 "k8s.io/api/batch/v1"
    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/labels"
    "k8s.io/apimachinery/pkg/selection"
)

const (
    radixJobNameEnvironmentVariable = "RADIX_JOB_NAME"
)

func (bh *batchHandler) createBatch(jobName string, jobComponent *radixv1.RadixDeployJobComponent, rd *radixv1.RadixDeployment, batchScheduleDescriptionSecret *corev1.Secret, batchScheduleDescription *models.BatchScheduleDescription) (*batchv1.Job, error) {
    var defaultJobComponentConfig *models.RadixJobComponentConfig
    if batchScheduleDescription != nil {
        defaultJobComponentConfig = &batchScheduleDescription.DefaultRadixJobComponentConfig
    }

    job, jobEnvVarsConfigMap, jobEnvVarsMetadataConfigMap, err := bh.buildBatchJobSpec(jobName, rd, jobComponent, batchScheduleDescriptionSecret, bh.model.Kube, defaultJobComponentConfig)
    if err != nil {
        return nil, err
    }
    namespace := bh.model.Env.RadixDeploymentNamespace
    //TODO - revise
    createdJobEnvVarsConfigMap, createdJobEnvVarsMetadataConfigMap, err := bh.createEnvVarsConfigMaps(namespace, jobEnvVarsConfigMap, jobEnvVarsMetadataConfigMap)
    if err != nil {
        return nil, err
    }
    createdJob, err := bh.model.KubeClient.BatchV1().Jobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
    if err != nil {
        return nil, err
    }
    err = bh.updateOwnerReferenceOfConfigMaps(createdJob, createdJobEnvVarsConfigMap, createdJobEnvVarsMetadataConfigMap)
    if err != nil {
        return nil, err
    }
    return createdJob, nil
}

func (bh *batchHandler) createEnvVarsConfigMaps(namespace string, jobEnvVarsConfigMap *corev1.ConfigMap, jobEnvVarsMetadataConfigMap *corev1.ConfigMap) (*corev1.ConfigMap, *corev1.ConfigMap, error) {
    createdJobEnvVarsConfigMap, err := bh.model.Kube.CreateConfigMap(namespace, jobEnvVarsConfigMap)
    if err != nil {
        return nil, nil, err
    }
    createdJobEnvVarsMetadataConfigMap, err := bh.model.Kube.CreateConfigMap(namespace, jobEnvVarsMetadataConfigMap)
    if err != nil {
        return nil, nil, err
    }
    return createdJobEnvVarsConfigMap, createdJobEnvVarsMetadataConfigMap, nil
}

func (bh *batchHandler) updateOwnerReferenceOfConfigMaps(ownerJob *batchv1.Job, configMaps ...*corev1.ConfigMap) error {
    jobOwnerReferences := getJobOwnerReferences(ownerJob)
    for _, configMap := range configMaps {
        configMap.OwnerReferences = jobOwnerReferences
    }
    return bh.model.Kube.UpdateConfigMap(ownerJob.ObjectMeta.GetNamespace(), configMaps...)
}

func (bh *batchHandler) deleteJob(job *batchv1.Job) error {
    fg := metav1.DeletePropagationBackground
    return bh.model.KubeClient.BatchV1().Jobs(job.Namespace).Delete(context.TODO(), job.Name, metav1.DeleteOptions{PropagationPolicy: &fg})
}

func (bh *batchHandler) getBatchByName(batchName string) (*batchv1.Job, error) {
    batches, err := bh.getAllBatches()
    if err != nil {
        return nil, err
    }

    batches = batches.Where(func(j *batchv1.Job) bool { return j.Name == batchName })

    if len(batches) == 1 {
        return batches[0], nil
    }

    return nil, jobErrors.NewNotFound("batch", batchName)
}

func (bh *batchHandler) getAllBatches() (models.JobList, error) {
    kubeBatches, err := bh.model.KubeClient.
        BatchV1().
        Jobs(bh.model.Env.RadixDeploymentNamespace).
        List(
            context.TODO(),
            metav1.ListOptions{
                LabelSelector: getLabelSelectorForJobComponentBatches(bh.model.Env.RadixComponentName),
            },
        )

    if err != nil {
        return nil, err
    }

    return slice.PointersOf(kubeBatches.Items).([]*batchv1.Job), nil
}

//getJobPods jobName is optional, when empty - returns all job-pods for the namespace
func (bh *batchHandler) getJobPods(jobName string) ([]corev1.Pod, error) {
    listOptions := metav1.ListOptions{}
    if jobName != "" {
        listOptions.LabelSelector = getLabelSelectorForJobPods(jobName)
    }
    podList, err := bh.model.KubeClient.
        CoreV1().
        Pods(bh.model.Env.RadixDeploymentNamespace).
        List(
            context.TODO(),
            listOptions,
        )

    if err != nil {
        return nil, err
    }

    return podList.Items, nil
}

func (bh *batchHandler) buildBatchJobSpec(batchName string, rd *radixv1.RadixDeployment, radixJobComponent *radixv1.RadixDeployJobComponent, batchScheduleDescriptionSecret *corev1.Secret, kubeutil *kube.Kube, defaultJobComponentConfig *models.RadixJobComponentConfig) (*batchv1.Job, *corev1.ConfigMap, *corev1.ConfigMap, error) {
    podSecurityContext := bh.model.SecurityContextBuilder.BuildPodSecurityContext(radixJobComponent)
    volumes, err := bh.getVolumes(rd.ObjectMeta.Namespace, rd.Spec.Environment, radixJobComponent, rd.Name, batchScheduleDescriptionSecret)
    if err != nil {
        return nil, nil, nil, err
    }
    containers, jobEnvVarsConfigMap, jobEnvVarsMetadataConfigMap, err := getContainersWithEnvVarsConfigMaps(kubeutil, rd, batchName, radixJobComponent, batchScheduleDescriptionSecret, defaultJobComponentConfig, bh.model.SecurityContextBuilder)
    if err != nil {
        return nil, nil, nil, err
    }

    return &batchv1.Job{
        ObjectMeta: metav1.ObjectMeta{
            Name: batchName,
            Labels: map[string]string{
                kube.RadixAppLabel:       rd.Spec.AppName,
                kube.RadixComponentLabel: radixJobComponent.Name,
                kube.RadixJobTypeLabel:   kube.RadixJobTypeJobSchedule,
                "radix-batch-name":       batchName,
            },
        },
        Spec: batchv1.JobSpec{
            BackoffLimit: numbers.Int32Ptr(0),
            Template: corev1.PodTemplateSpec{
                ObjectMeta: metav1.ObjectMeta{
                    Labels: map[string]string{
                        kube.RadixAppLabel:     rd.Spec.AppName,
                        kube.RadixJobTypeLabel: kube.RadixJobTypeJobSchedule,
                        "radix-batch-name":     batchName,
                    },
                    Namespace: rd.ObjectMeta.Namespace,
                },
                Spec: corev1.PodSpec{
                    Containers:       containers,
                    Volumes:          volumes,
                    SecurityContext:  podSecurityContext,
                    RestartPolicy:    corev1.RestartPolicyNever,
                    ImagePullSecrets: rd.Spec.ImagePullSecrets,
                },
            },
        },
    }, jobEnvVarsConfigMap, jobEnvVarsMetadataConfigMap, nil
}

func getContainersWithEnvVarsConfigMaps(kubeUtils *kube.Kube, rd *radixv1.RadixDeployment, jobName string, radixJobComponent *radixv1.RadixDeployJobComponent, batchScheduleDescriptionSecret *corev1.Secret, jobComponentConfig *models.RadixJobComponentConfig, securityContextBuilder deployment.SecurityContextBuilder) ([]corev1.Container, *corev1.ConfigMap, *corev1.ConfigMap, error) {
    environmentVariables, jobEnvVarsConfigMap, jobEnvVarsMetadataConfigMap, err := buildEnvironmentVariablesWithEnvVarsConfigMaps(kubeUtils, rd, jobName, radixJobComponent)
    if err != nil {
        return nil, nil, nil, err
    }
    ports := getContainerPorts(radixJobComponent)
    volumeMounts, err := getVolumeMounts(batchScheduleDescriptionSecret)
    if err != nil {
        return nil, nil, nil, err
    }
    resources := getResourceRequirements(radixJobComponent, jobComponentConfig)
    containerSecurityContext := securityContextBuilder.BuildContainerSecurityContext(radixJobComponent)

    container := corev1.Container{
        Name:            radixJobComponent.Name,
        Image:           radixJobComponent.Image,
        ImagePullPolicy: corev1.PullAlways,
        Env:             environmentVariables,
        Ports:           ports,
        VolumeMounts:    volumeMounts,
        SecurityContext: containerSecurityContext,
        Resources:       resources,
    }

    return []corev1.Container{container}, jobEnvVarsConfigMap, jobEnvVarsMetadataConfigMap, nil
}

func buildEnvironmentVariablesWithEnvVarsConfigMaps(kubeUtils *kube.Kube, rd *radixv1.RadixDeployment, jobName string, radixJobComponent *radixv1.RadixDeployJobComponent) ([]corev1.EnvVar, *corev1.ConfigMap, *corev1.ConfigMap, error) {
    envVarsConfigMap, _, envVarsMetadataMap, err := kubeUtils.GetEnvVarsConfigMapAndMetadataMap(rd.GetNamespace(), radixJobComponent.GetName()) //env-vars metadata for jobComponent to use it for job's env-vars metadata
    if err != nil {
        return nil, nil, nil, err
    }
    if envVarsMetadataMap == nil {
        envVarsMetadataMap = map[string]kube.EnvVarMetadata{}
    }
    jobEnvVarsConfigMap := kube.BuildRadixConfigEnvVarsConfigMap(rd.GetName(), jobName) //build env-vars config-name with name 'env-vars-JOB_NAME'
    jobEnvVarsConfigMap.Data = envVarsConfigMap.Data
    jobEnvVarsMetadataConfigMap := kube.BuildRadixConfigEnvVarsMetadataConfigMap(rd.GetName(), jobName) //build env-vars metadata config-name with name and 'env-vars-metadata-JOB_NAME'

    environmentVariables, err := deployment.GetEnvironmentVariables(kubeUtils, rd.Spec.AppName, rd, radixJobComponent)
    if err != nil {
        return nil, nil, nil, err
    }
    environmentVariables = append(environmentVariables, corev1.EnvVar{Name: radixJobNameEnvironmentVariable, ValueFrom: &corev1.EnvVarSource{
        FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels['job-name']"},
    }})

    err = kube.SetEnvVarsMetadataMapToConfigMap(jobEnvVarsMetadataConfigMap, envVarsMetadataMap) //use env-vars metadata config-map, individual for each job
    if err != nil {
        return nil, nil, nil, err
    }

    return environmentVariables, jobEnvVarsConfigMap, jobEnvVarsMetadataConfigMap, nil
}

func getJobOwnerReferences(job *batchv1.Job) []metav1.OwnerReference {
    return []metav1.OwnerReference{
        {
            APIVersion: "batch/v1",
            Kind:       "Job",
            Name:       job.GetName(),
            UID:        job.UID,
            Controller: radixutils.BoolPtr(true),
        },
    }
}

func getVolumeMounts(batchScheduleDescriptionSecret *corev1.Secret) ([]corev1.VolumeMount, error) {
    volumeMounts := make([]corev1.VolumeMount, 0)
    if batchScheduleDescriptionSecret != nil {
        volumeMounts = append(volumeMounts, corev1.VolumeMount{
            Name:      batchScheduleDescriptionSecretPropertyName,
            ReadOnly:  true,
            MountPath: "/mnt/secrets",
        })
    }

    return volumeMounts, nil
}

func (bh *batchHandler) getVolumes(namespace, environment string, radixJobComponent *radixv1.RadixDeployJobComponent, radixDeploymentName string, batchScheduleDescriptionSecret *corev1.Secret) ([]corev1.Volume, error) {
    volumes, err := deployment.GetVolumes(bh.model.KubeClient, bh.model.Kube, namespace, environment, radixJobComponent, radixDeploymentName)
    if err != nil {
        return nil, err
    }

    if batchScheduleDescriptionSecret != nil {
        volumes = append(volumes, *getBatchScheduleDescriptionVolume(batchScheduleDescriptionSecret.Name))
    }

    return volumes, nil
}

func getResourceRequirements(radixJobComponent *radixv1.RadixDeployJobComponent, jobComponentConfig *models.RadixJobComponentConfig) corev1.ResourceRequirements {
    if jobComponentConfig != nil && jobComponentConfig.Resources != nil {
        return operatorUtils.BuildResourceRequirement(jobComponentConfig.Resources)
    } else {
        return operatorUtils.GetResourceRequirements(radixJobComponent)
    }
}

func getBatchScheduleDescriptionVolume(secretName string) *corev1.Volume {
    volume := &corev1.Volume{
        Name: jobDefaults.BatchScheduleDescriptionPropertyName,
        VolumeSource: corev1.VolumeSource{
            Secret: &corev1.SecretVolumeSource{
                SecretName: secretName,
            },
        },
    }
    return volume
}

func getContainerPorts(radixJobComponent *radixv1.RadixDeployJobComponent) []corev1.ContainerPort {
    var ports []corev1.ContainerPort
    for _, v := range radixJobComponent.Ports {
        containerPort := corev1.ContainerPort{
            Name:          v.Name,
            ContainerPort: v.Port,
        }
        ports = append(ports, containerPort)
    }
    return ports
}

func getLabelSelectorForJobComponentBatches(componentName string) string {
    componentRequirement, _ := labels.NewRequirement(kube.RadixComponentLabel, selection.Equals, []string{componentName})
    jobTypeRequirement, _ := labels.NewRequirement(kube.RadixJobTypeLabel, selection.Equals, []string{kube.RadixJobTypeJobSchedule})
    batchNameRequirement, _ := labels.NewRequirement("radix-batch-name", selection.Exists, nil) //TODO kube.Label...
    return labels.NewSelector().Add(*componentRequirement, *jobTypeRequirement, *batchNameRequirement).String()
}

func getLabelSelectorForJobPods(jobName string) string {
    return labels.SelectorFromSet(map[string]string{
        jobDefaults.K8sJobNameLabel: jobName,
    }).String()
}
