<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Api\Core\V1\EnvVar;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Probe;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ResourceRequirements;
use KubernetesRuntime\AbstractModel;

/**
 * AppContainer defines a simplified container specification
 *
 * @property string|null $name
 * @property string|null $image
 * @property array<string>|null $command
 * @property array<EnvVar>|null $env
 * @property ResourceRequirements|null $resources
 * @property Probe|null $livenessProbe
 * @property Probe|null $readinessProbe
 * @property Probe|null $startupProbe
 */
class AppContainer extends AbstractModel
{
    /**
     * Name of the container
     *
     * @var string|null
     */
    public $name = null;

    /**
     * Image to use for the container
     *
     * @var string|null
     */
    public $image = null;

    /**
     * Command to run in the container
     *
     * @var array<string>|null
     */
    public $command = null;

    /**
     * Environment variables
     *
     * @var array<EnvVar>|null
     */
    public $env = null;

    /**
     * Resources defines the compute resources
     *
     * @var ResourceRequirements|null
     */
    public $resources = null;

    /**
     * LivenessProbe defines how to check if the container is alive
     *
     * @var Probe|null
     */
    public $livenessProbe = null;

    /**
     * ReadinessProbe defines how to check if the container is ready
     *
     * @var Probe|null
     */
    public $readinessProbe = null;

    /**
     * StartupProbe defines how to check if the container has started
     *
     * @var Probe|null
     */
    public $startupProbe = null;
}
