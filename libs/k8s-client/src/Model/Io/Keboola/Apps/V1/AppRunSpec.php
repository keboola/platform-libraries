<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppRunSpec defines the desired state of AppRun
 *
 * @property PodReference|null $podRef
 * @property AppReference|null $appRef
 * @property string|null $createdAt
 * @property string|null $startedAt
 * @property string|null $stoppedAt
 * @property string|null $state
 * @property string|null $startupLogs
 * @property string|null $runtimeSize
 * @property string|null $configVersion
 */
class AppRunSpec extends AbstractModel
{
    /**
     * PodRef is a reference to the Pod this AppRun tracks
     */
    public PodReference|null $podRef = null;

    /**
     * AppRef is a reference to the App resource this run belongs to
     */
    public AppReference|null $appRef = null;

    /**
     * CreatedAt is the timestamp when this run was created
     */
    public string|null $createdAt = null;

    /**
     * StartedAt is the timestamp when this run started
     */
    public string|null $startedAt = null;

    /**
     * StoppedAt is the timestamp when this run stopped
     */
    public string|null $stoppedAt = null;

    /**
     * State represents the current state of the run
     * Possible values: Starting, Running, Failed, Finished
     */
    public string|null $state = null;

    /**
     * StartupLogs contains the startup logs from the run
     */
    public string|null $startupLogs = null;

    /**
     * RuntimeSize specifies the dynamic backend size for this run
     */
    public string|null $runtimeSize = null;

    /**
     * ConfigVersion specifies the configuration version used for this run
     */
    public string|null $configVersion = null;
}
