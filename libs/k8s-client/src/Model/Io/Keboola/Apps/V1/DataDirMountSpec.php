<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * DataDirMountSpec defines where to mount the data directory in a specific container
 */
class DataDirMountSpec extends AbstractModel
{
    /**
     * @var string|null
     */
    public $container = null;

    /**
     * @var string|null
     */
    public $path = null;
}
