<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * SetEnvSpec defines which environment variable will contain the storage token
 */
class SetEnvSpec extends AbstractModel
{
    public string|null $envName = null;
}
