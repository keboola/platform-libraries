<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppPodSpec defines a subset of PodSpec
 *
 * @property int|null $terminationGracePeriodSeconds
 * @property array<AppContainer>|null $containers
 * @property string|null $restartPolicy
 * @property array<string, string>|null $annotations
 */
class AppPodSpec extends AbstractModel
{
    /**
     * TerminationGracePeriodSeconds is the duration in seconds the pod needs to terminate gracefully
     *
     * @var int|null
     */
    public $terminationGracePeriodSeconds = null;

    /**
     * Containers define the containers to run in the pod
     *
     * @var array<AppContainer>|null
     */
    public $containers = null;

    /**
     * RestartPolicy defines pod restart policy
     *
     * @var string|null
     */
    public $restartPolicy = null;

    /**
     * Annotations defines annotations to be added to the pod
     *
     * @var array<string, string>|null
     */
    public $annotations = null;
}
