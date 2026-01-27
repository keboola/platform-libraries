<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Api\Core\V1\LocalObjectReference;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Condition;
use KubernetesRuntime\AbstractModel;

/**
 * AppStatus defines the observed state of App
 *
 * @property int|null $observedGeneration
 * @property string|null $currentState
 * @property int|null $readyReplicas
 * @property int|null $updatedReplicas
 * @property string|null $lastStartedTime
 * @property LocalObjectReference|null $storageTokenRef
 * @property LocalObjectReference|null $appsProxyServiceRef
 * @property array<Condition>|null $conditions
 */
class AppStatus extends AbstractModel
{
    /**
     * ObservedGeneration is the most recent generation observed for this App
     */
    public int|null $observedGeneration = null;

    /**
     * CurrentState represents the current state of the App
     * Possible values: Stopped, Running, Starting, Stopping
     */
    public string|null $currentState = null;

    /**
     * ReadyReplicas indicates the number of ready replicas
     */
    public int|null $readyReplicas = null;

    /**
     * UpdatedReplicas indicates the number of updated replicas
     */
    public int|null $updatedReplicas = null;

    /**
     * LastStartedTime is the timestamp when the app last transitioned to Running state
     */
    public string|null $lastStartedTime = null;

    /**
     * StorageTokenRef contains a reference to the storage token currently used by the app
     */
    public LocalObjectReference|null $storageTokenRef = null;

    /**
     * AppsProxyServiceRef contains a reference to the service used for apps proxy ingress
     */
    public LocalObjectReference|null $appsProxyServiceRef = null;

    /**
     * Conditions represents the latest available observations of the app's current state
     *
     * @var array<Condition>|null
     */
    public array|null $conditions = null;
}
