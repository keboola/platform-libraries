<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * MountPathSpec defines where to mount the storage token in a specific container
 */
class MountPathSpec extends AbstractModel
{
    /**
     * @var string
     */
    public $container = null;

    /**
     * @var string
     */
    public $path = null;
}
