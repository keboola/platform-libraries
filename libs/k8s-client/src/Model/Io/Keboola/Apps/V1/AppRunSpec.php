<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Time;
use KubernetesRuntime\AbstractModel;

/**
 * AppRunSpec defines the desired state of AppRun
 */
class AppRunSpec extends AbstractModel
{
    /**
     * PodRef is a reference to the Pod this AppRun tracks
     *
     * @var PodReference
     */
    public $podRef = null;

    /**
     * AppRef is a reference to the App resource this run belongs to
     *
     * @var AppReference
     */
    public $appRef = null;

    /**
     * CreatedAt is the timestamp when this run was created
     *
     * @var Time
     */
    public $createdAt = null;

    /**
     * StartedAt is the timestamp when this run started
     *
     * @var Time|null
     */
    public $startedAt = null;

    /**
     * StoppedAt is the timestamp when this run stopped
     *
     * @var Time|null
     */
    public $stoppedAt = null;

    /**
     * State represents the current state of the run
     * Possible values: Starting, Running, Failed, Finished
     *
     * @var string
     */
    public $state = null;

    /**
     * StartupLogs contains the startup logs from the run
     *
     * @var string|null
     */
    public $startupLogs = null;

    /**
     * RuntimeSize specifies the dynamic backend size for this run
     *
     * @var string|null
     */
    public $runtimeSize = null;

    /**
     * ConfigVersion specifies the configuration version used for this run
     *
     * @var string|null
     */
    public $configVersion = null;
}
