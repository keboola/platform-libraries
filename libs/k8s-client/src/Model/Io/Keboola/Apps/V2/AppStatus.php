<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Api\Core\V1\LocalObjectReference;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Condition;
use KubernetesRuntime\AbstractModel;

/**
 * AppStatus defines the observed state of App
 */
class AppStatus extends AbstractModel
{
    /**
     * ObservedGeneration is the most recent generation observed for this App
     *
     * @var integer|null
     */
    public $observedGeneration = null;

    /**
     * CurrentState represents the current state of the App
     * Possible values: Stopped, Running, Starting, Stopping
     *
     * @var string|null
     */
    public $currentState = null;

    /**
     * ReadyReplicas indicates the number of ready replicas
     *
     * @var integer|null
     */
    public $readyReplicas = null;

    /**
     * UpdatedReplicas indicates the number of updated replicas
     *
     * @var integer|null
     */
    public $updatedReplicas = null;

    /**
     * LastStartedTime is the timestamp when the app last transitioned to Running state
     *
     * @var string|null
     */
    public $lastStartedTime = null;

    /**
     * RunStartRequestedAt is set (microsecond precision) when spec.state transitions to Running
     * and cleared when the app stops.
     *
     * @var string|null
     */
    public $runStartRequestedAt = null;

    /**
     * AppsProxy holds status for the apps proxy ingress feature.
     *
     * @var AppsProxyStatus|null
     */
    public $appsProxy = null;

    /**
     * AppsProxyServiceRef contains a reference to the service used for apps proxy ingress
     *
     * @deprecated Use appsProxy.serviceRef instead.
     * @var LocalObjectReference|null
     */
    public $appsProxyServiceRef = null;

    /**
     * E2bSandbox holds status for E2B-backed apps (backend=e2bSandbox only).
     *
     * @var E2bSandboxStatus|null
     */
    public $e2bSandbox = null;

    /**
     * Conditions represents the latest available observations of the app's current state
     *
     * @var Condition[]|null
     */
    public $conditions = null;
}
