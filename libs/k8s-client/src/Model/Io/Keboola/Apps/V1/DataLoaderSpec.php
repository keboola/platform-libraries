<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * DataLoaderSpec defines configuration for the data loader feature
 */
class DataLoaderSpec extends AbstractModel
{
    /**
     * @var string
     */
    public $branchId = null;

    /**
     * @var string
     */
    public $componentId = null;

    /**
     * @var string
     */
    public $configId = null;

    /**
     * @var int|null
     */
    public $port = null;
}
