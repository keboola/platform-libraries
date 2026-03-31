<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * AppSpec defines the desired state of App
 */
class AppSpec extends AbstractModel
{
    /**
     * AppID is the unique identifier of the app
     *
     * @var string
     */
    public $appId = null;

    /**
     * ProjectID is the ID of the project this app belongs to
     *
     * @var string
     */
    public $projectId = null;

    /**
     * State defines whether the app is running or stopped (Running or Stopped)
     *
     * @var string
     */
    public $state = null;

    /**
     * Replicas defines the number of app instances to run (default: 1, minimum: 1)
     *
     * @var integer
     */
    public $replicas = null;

    /**
     * AutoRestartEnabled determines if the app should automatically restart on startup probe failures.
     * Default is true. When set to false during deployment of new versions, the PodFailure controller
     * will stop the app if it enters CrashLoopBackOff, preventing infinite restart loops.
     *
     * @var boolean|null
     */
    public $autoRestartEnabled = null;

    /**
     * RestartRequestedAt is the timestamp when the restart was requested.
     *
     * @var string|null
     */
    public $restartRequestedAt = null;

    /**
     * RuntimeSize specifies the dynamic backend size for this app
     *
     * @deprecated Use runtime.size instead
     * @var string|null
     */
    public $runtimeSize = null;

    /**
     * @var AppRuntime|null
     */
    public $runtime = null;

    /**
     * ContainerSpec defines the container specification for the app.
     * In v2, apps run a single container (unlike v1 which supported
     * multiple containers via podSpec).
     *
     * @var ContainerSpec
     */
    public $containerSpec = null;

    /**
     * Features defines optional features that can be enabled for the app
     *
     * @var AppFeatures|null
     */
    public $features = null;
}
