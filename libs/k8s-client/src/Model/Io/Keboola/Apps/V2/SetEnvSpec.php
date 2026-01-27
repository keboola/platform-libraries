<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * SetEnvSpec defines which container and environment variable will contain the storage token
 */
class SetEnvSpec extends AbstractModel
{
    /**
     * Container is the name of the container to set the environment variable in.
     *
     * Deprecated: In v2, container field is ignored as there is only one container
     * defined in containerSpec.
     */
    public string|null $container = null;

    public string|null $envName = null;
}
