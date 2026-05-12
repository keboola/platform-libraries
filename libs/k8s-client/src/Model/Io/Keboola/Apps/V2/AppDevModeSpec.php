<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * AppDevModeSpec configures dev-mode behaviour for the app (AppSpec.devMode).
 */
class AppDevModeSpec extends AbstractModel
{
    /**
     * Enabled toggles dev mode. When true, the runtime loads the dev
     * supervisord profile and the in-pod git-watcher runs. Default false.
     *
     * @var bool|null
     */
    public $enabled = null;

    /**
     * GitPollInterval is how often the in-pod git-watcher fetches and resets
     * to origin/<branch>. ISO-8601 duration string (e.g. "1s", "30s", "5m").
     * Default 1s, min 1s, max 5m (enforced at admission via CRD CEL).
     *
     * @var string|null
     */
    public $gitPollInterval = null;

    /**
     * AutoRunSetupOnDepChange controls whether the in-pod git-watcher runs
     * setup-dev.sh and restarts the app program when a tracked dependency
     * file changes during a poll cycle. Default true.
     *
     * @var bool|null
     */
    public $autoRunSetupOnDepChange = null;
}
