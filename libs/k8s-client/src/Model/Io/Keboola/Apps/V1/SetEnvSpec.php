<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * SetEnvSpec defines which container and environment variable will contain the storage token
 */
class SetEnvSpec extends AbstractModel
{
    /**
     * @var string
     */
    public $container = null;

    /**
     * @var string
     */
    public $envName = null;
}
