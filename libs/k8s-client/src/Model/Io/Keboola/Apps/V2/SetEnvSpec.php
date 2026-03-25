<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * SetEnvSpec defines which environment variable will contain the storage token
 */
class SetEnvSpec extends AbstractModel
{
    /**
     * Container is the name of the container to set the environment variable in
     *
     * @var string
     */
    public $container = null;

    /**
     * EnvName is the name of the environment variable to set with the token value
     *
     * @var string
     */
    public $envName = null;
}
