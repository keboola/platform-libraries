<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppSpec defines the desired state of App
 *
 * @property string|null $appId
 * @property string|null $projectId
 * @property string|null $state
 * @property int|null $replicas
 * @property AppPodSpec|null $podSpec
 * @property AppFeatures|null $features
 */
class AppSpec extends AbstractModel
{
    /**
     * AppID is the unique identifier of the app
     */
    public string|null $appId = null;

    /**
     * ProjectID is the ID of the project this app belongs to
     */
    public string|null $projectId = null;

    /**
     * State defines whether the app is running or stopped (Running or Stopped)
     */
    public string|null $state = null;

    /**
     * Replicas defines the number of app instances to run (default: 1, minimum: 1)
     */
    public int|null $replicas = null;

    /**
     * PodSpec defines the simplified pod specification for the app
     */
    public AppPodSpec|null $podSpec = null;

    /**
     * Features defines optional features that can be enabled for the app
     */
    public AppFeatures|null $features = null;
}
