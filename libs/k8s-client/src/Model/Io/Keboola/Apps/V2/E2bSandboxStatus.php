<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Time;
use KubernetesRuntime\AbstractModel;

/**
 * E2bSandboxStatus defines the observed state of the E2B sandbox
 */
class E2bSandboxStatus extends AbstractModel
{
    /**
     * Name is the name of the E2B sandbox resource
     *
     * @var string
     */
    public $name = null;

    /**
     * SandboxID is the ID of the E2B sandbox
     *
     * @var string
     */
    public $sandboxID = null;

    /**
     * StartupLaunchedAt is the timestamp when the sandbox startup was launched
     *
     * @var Time
     */
    public $startupLaunchedAt = null;

    /**
     * StartupProbeFailures is the number of startup probe failures
     *
     * @var integer
     */
    public $startupProbeFailures = null;

    /**
     * SyncedFileHashes contains the hashes of synced files
     *
     * @var array
     */
    public $syncedFileHashes = null;
}
