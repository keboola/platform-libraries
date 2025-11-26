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
 * @property bool|null $autoRestartEnabled
 * @property AppPodSpec|null $podSpec
 * @property AppFeatures|null $features
 */
class AppSpec extends AbstractModel
{
    /**
     * AppID is the unique identifier of the app
     *
     * @var string|null
     */
    public $appId = null;

    /**
     * ProjectID is the ID of the project this app belongs to
     *
     * @var string|null
     */
    public $projectId = null;

    /**
     * State defines whether the app is running or stopped (Running or Stopped)
     *
     * @var string|null
     */
    public $state = null;

    /**
     * Replicas defines the number of app instances to run (default: 1, minimum: 1)
     *
     * @var int|null
     */
    public $replicas = null;

    /**
     * AutoRestartEnabled determines if the app should automatically restart on startup probe failures.
     * Default is true. When set to false during deployment of new versions, the PodFailure controller
     * will stop the app if it enters CrashLoopBackOff, preventing infinite restart loops.
     *
     * @var bool|null
     */
    public $autoRestartEnabled = null;

    /**
     * PodSpec defines the simplified pod specification for the app
     *
     * @var AppPodSpec|null
     */
    public $podSpec = null;

    /**
     * Features defines optional features that can be enabled for the app
     *
     * @var AppFeatures|null
     */
    public $features = null;
}
