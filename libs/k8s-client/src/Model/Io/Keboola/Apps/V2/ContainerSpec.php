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
 *
 * @property string|null $image
 * @property array<string>|null $command
 * @property array<EnvVar>|null $env
 * @property Probe|null $livenessProbe
 * @property Probe|null $readinessProbe
 * @property Probe|null $startupProbe
 */
class ContainerSpec extends AbstractModel
{
    /**
     * Image to use for the container
     */
    public string|null $image = null;

    /**
     * Command to run in the container
     *
     * @var array<string>|null
     */
    public array|null $command = null;

    /**
     * Environment variables
     *
     * @var array<EnvVar>|null
     */
    public array|null $env = null;

    /**
     * LivenessProbe defines how to check if the container is alive
     */
    public Probe|null $livenessProbe = null;

    /**
     * ReadinessProbe defines how to check if the container is ready
     */
    public Probe|null $readinessProbe = null;

    /**
     * StartupProbe defines how to check if the container has started
     */
    public Probe|null $startupProbe = null;
}
