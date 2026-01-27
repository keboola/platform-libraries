<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * DataDirMountSpec defines where to mount the data directory in a specific container
 */
class DataDirMountSpec extends AbstractModel
{
    public string|null $container = null;
    public string|null $path = null;
}
