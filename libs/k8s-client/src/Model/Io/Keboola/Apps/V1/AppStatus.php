<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Api\Core\V1\LocalObjectReference;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Condition;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Time;
use KubernetesRuntime\AbstractModel;

/**
 * AppStatus defines the observed state of App
 *
 * @property int|null $observedGeneration
 * @property string|null $currentState
 * @property int|null $readyReplicas
 * @property int|null $updatedReplicas
 * @property Time|null $lastStartedTime
 * @property LocalObjectReference|null $storageTokenRef
 * @property LocalObjectReference|null $appsProxyServiceRef
 * @property array<Condition>|null $conditions
 */
class AppStatus extends AbstractModel
{
    /**
     * ObservedGeneration is the most recent generation observed for this App
     *
     * @var int|null
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
     * @var int|null
     */
    public $readyReplicas = null;

    /**
     * UpdatedReplicas indicates the number of updated replicas
     *
     * @var int|null
     */
    public $updatedReplicas = null;

    /**
     * LastStartedTime is the timestamp when the app last transitioned to Running state
     *
     * @var Time|null
     */
    public $lastStartedTime = null;

    /**
     * StorageTokenRef contains a reference to the storage token currently used by the app
     *
     * @var LocalObjectReference|null
     */
    public $storageTokenRef = null;

    /**
     * AppsProxyServiceRef contains a reference to the service used for apps proxy ingress
     *
     * @var LocalObjectReference|null
     */
    public $appsProxyServiceRef = null;

    /**
     * Conditions represents the latest available observations of the app's current state
     *
     * @var array<Condition>|null
     */
    public $conditions = null;
}
