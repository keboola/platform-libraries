<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * DataLoaderSpec defines configuration for the data loader feature
 */
class DataLoaderSpec extends AbstractModel
{
    public string|null $branchId = null;
    public string|null $componentId = null;
    public string|null $configId = null;
    public int|null $port = null;
}
