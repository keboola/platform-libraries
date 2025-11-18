<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppSpec defines the desired state of App
 *
 * @property string|null $appId
 * @property string|null $configId
 * @property string|null $projectId
 * @property string|null $state
 * @property int|null $replicas
 * @property AppFeatures|null $features
 * @property array<string, mixed>|null $podSpec
 */
class AppSpec extends AbstractModel
{
    /**
     * Application ID - unique identifier of the app
     */
    public string|null $appId = null;

    /**
     * Configuration ID - displayed in printer columns
     */
    public string|null $configId = null;

    /**
     * Project ID where the app belongs
     */
    public string|null $projectId = null;

    /**
     * Desired state of the app (Running or Stopped)
     */
    public string|null $state = null;

    /**
     * Number of replicas (default: 1, minimum: 1)
     */
    public int|null $replicas = null;

    /**
     * Features configuration for the app
     */
    public AppFeatures|null $features = null;

    /**
     * Pod specification for the app container
     * This contains the simplified Kubernetes PodSpec fields
     *
     * @var array<string, mixed>|null
     */
    public array|null $podSpec = null;
}
