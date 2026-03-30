<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Time;
use KubernetesRuntime\AbstractModel;

/**
 * E2bSandboxStatus holds status for E2B-backed apps
 */
class E2bSandboxStatus extends AbstractModel
{
    /**
     * Name of the child E2bSandbox resource.
     *
     * @var string
     */
    public $name = null;

    /**
     * SandboxID of the E2B sandbox this state corresponds to.
     *
     * @var string
     */
    public $sandboxID = null;

    /**
     * StartupLaunchedAt records when the startup script was launched.
     *
     * @var Time
     */
    public $startupLaunchedAt = null;

    /**
     * StartupProbeFailures tracks the number of consecutive startup probe failures.
     *
     * @var integer
     */
    public $startupProbeFailures = null;

    /**
     * SyncedFileHashes records the content hash of each file last uploaded to the sandbox.
     *
     * @var array<string, string>
     */
    public $syncedFileHashes = null;

    /**
     * TemplateBuildID is the E2B build ID returned by BuildTemplate.
     *
     * @var string
     */
    public $templateBuildID = null;
}
