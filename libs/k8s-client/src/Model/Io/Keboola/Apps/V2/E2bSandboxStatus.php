<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

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
     * @var string|null
     */
    public $sandboxID = null;

    /**
     * StartupLaunchedAt records when the startup script was launched.
     *
     * @var string|null
     */
    public $startupLaunchedAt = null;

    /**
     * StartupProbeFailures tracks the number of consecutive startup probe failures.
     *
     * @var integer|null
     */
    public $startupProbeFailures = null;

    /**
     * SyncedFileHashes records the content hash of each file last uploaded to the sandbox.
     *
     * @var array<string, string>|null
     */
    public $syncedFileHashes = null;

    /**
     * TemplateBuildID is the E2B build ID returned by BuildTemplate.
     *
     * @var string|null
     */
    public $templateBuildID = null;
}
