<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * ConfigMountSpec defines configuration for mounting config files
 */
class ConfigMountSpec extends AbstractModel
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
     * @var string
     */
    public $configVersion = null;

    /**
     * @var ConfigMountItemSpec[]
     */
    public $mount = null;
}
