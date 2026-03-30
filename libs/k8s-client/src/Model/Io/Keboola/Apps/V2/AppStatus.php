<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Api\Core\V1\LocalObjectReference;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Condition;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Time;
use KubernetesRuntime\AbstractModel;

/**
 * AppStatus defines the observed state of App
 */
class AppStatus extends AbstractModel
{
    /**
     * ObservedGeneration is the most recent generation observed for this App
     *
     * @var integer
     */
    public $observedGeneration = null;

    /**
     * CurrentState represents the current state of the App
     * Possible values: Stopped, Running, Starting, Stopping
     *
     * @var string
     */
    public $currentState = null;

    /**
     * ReadyReplicas indicates the number of ready replicas
     *
     * @var integer
     */
    public $readyReplicas = null;

    /**
     * UpdatedReplicas indicates the number of updated replicas
     *
     * @var integer
     */
    public $updatedReplicas = null;

    /**
     * LastStartedTime is the timestamp when the app last transitioned to Running state
     *
     * @var Time
     */
    public $lastStartedTime = null;

    /**
     * RunStartRequestedAt is set (microsecond precision) when spec.state transitions to Running
     * and cleared when the app stops.
     *
     * @var Time
     */
    public $runStartRequestedAt = null;

    /**
     * AppsProxy holds status for the apps proxy ingress feature.
     *
     * @var AppsProxyStatus
     */
    public $appsProxy = null;

    /**
     * AppsProxyServiceRef contains a reference to the service used for apps proxy ingress
     *
     * @deprecated Use appsProxy.serviceRef instead.
     * @var LocalObjectReference
     */
    public $appsProxyServiceRef = null;

    /**
     * E2bSandbox holds status for E2B-backed apps (backend=e2bSandbox only).
     *
     * @var E2bSandboxStatus
     */
    public $e2bSandbox = null;

    /**
     * Conditions represents the latest available observations of the app's current state
     *
     * @var Condition[]
     */
    public $conditions = null;
}
