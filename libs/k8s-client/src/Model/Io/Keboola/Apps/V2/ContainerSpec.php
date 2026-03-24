<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Api\Core\V1\EnvVar;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Probe;
use KubernetesRuntime\AbstractModel;

/**
 * ContainerSpec defines a simplified container specification for v2 API.
 * Unlike v1's AppContainer, this does not include name (single container)
 * or resources (managed by runtimeSize).
 */
class ContainerSpec extends AbstractModel
{
    /**
     * Image to use for the container
     *
     * @var string
     */
    public $image = null;

    /**
     * Command to run in the container
     *
     * @var string[]
     */
    public $command = null;

    /**
     * Environment variables
     *
     * @var EnvVar[]
     */
    public $env = null;

    /**
     * LivenessProbe defines how to check if the container is alive
     *
     * @var Probe
     */
    public $livenessProbe = null;

    /**
     * ReadinessProbe defines how to check if the container is ready
     *
     * @var Probe
     */
    public $readinessProbe = null;

    /**
     * StartupProbe defines how to check if the container has started
     *
     * @var Probe
     */
    public $startupProbe = null;
}
