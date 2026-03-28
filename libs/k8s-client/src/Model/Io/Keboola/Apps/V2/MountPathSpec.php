<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * MountPathSpec defines where to mount the storage token in a specific container
 */
class MountPathSpec extends AbstractModel
{
    /**
     * Container is the name of the container to mount the token in
     *
     * @var string
     */
    public $container = null;

    /**
     * Path is the mount path for the token in the container
     *
     * @var string
     */
    public $path = null;
}
