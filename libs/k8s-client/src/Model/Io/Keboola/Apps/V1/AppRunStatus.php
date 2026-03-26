<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Condition;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Time;
use KubernetesRuntime\AbstractModel;

/**
 * AppRunStatus defines the observed state of AppRun
 */
class AppRunStatus extends AbstractModel
{
    /**
     * SyncedAt is the timestamp when this AppRun was synced to the backend
     *
     * @var Time
     */
    public $syncedAt = null;

    /**
     * Conditions represent the latest available observations of the AppRun's current state
     *
     * @var Condition[]
     */
    public $conditions = null;
}
