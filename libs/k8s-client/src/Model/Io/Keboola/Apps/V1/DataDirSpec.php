<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * DataDirSpec defines configuration for the data directory feature
 */
class DataDirSpec extends AbstractModel
{
    /**
     * @var array<DataDirMountSpec>|null
     */
    public $mount = null;

    /**
     * @var DataLoaderSpec|null
     */
    public $dataLoader = null;
}
