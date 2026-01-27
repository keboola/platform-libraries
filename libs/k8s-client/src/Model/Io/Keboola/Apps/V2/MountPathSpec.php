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
     * Container is the name of the container to mount the storage token in.
     *
     * Deprecated: In v2, container field is ignored as there is only one container
     * defined in containerSpec.
     */
    public string|null $container = null;

    public string|null $path = null;
}
