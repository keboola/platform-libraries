<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppStatus defines the observed state of App
 *
 * This is populated by the Kubernetes operator and represents
 * the current state of the application
 *
 * @property array<string, mixed>|null $appsProxyServiceRef
 * @property array<string, mixed>|null $conditions
 * @property string|null $currentState
 * @property string|null $lastStartedTime
 * @property int|null $observedGeneration
 * @property int|null $readyReplicas
 * @property array<string, mixed>|null $storageTokenRef
 * @property int|null $updatedReplicas
 */
class AppStatus extends AbstractModel
{
    /**
     * AppsProxyServiceRef contains a reference to the service used for apps proxy ingress
     *
     * @var array<string, mixed>|null
     */
    public array|null $appsProxyServiceRef = null;

    /**
     * Conditions represents the latest available observations of the app's current state
     *
     * @var array<string, mixed>|null
     */
    public array|null $conditions = null;

    /**
     * CurrentState represents the current state of the App
     * Possible values: Stopped, Running, Starting, Stopping
     */
    public string|null $currentState = null;

    /**
     * LastStartedTime is the timestamp when the app last transitioned to Running state
     */
    public string|null $lastStartedTime = null;

    /**
     * ObservedGeneration is the most recent generation observed for this App
     */
    public int|null $observedGeneration = null;

    /**
     * ReadyReplicas indicates the number of ready replicas
     */
    public int|null $readyReplicas = null;

    /**
     * StorageTokenRef contains a reference to the storage token currently used by the app
     *
     * @var array<string, mixed>|null
     */
    public array|null $storageTokenRef = null;

    /**
     * UpdatedReplicas indicates the number of updated replicas
     */
    public int|null $updatedReplicas = null;
}
