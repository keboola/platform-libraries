<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * ConfigMountSpec defines configuration for mounting config files
 */
class ConfigMountSpec extends AbstractModel
{
    /**
     * @var string|null
     */
    public $branchId = null;

    /**
     * @var string|null
     */
    public $componentId = null;

    /**
     * @var string|null
     */
    public $configId = null;

    /**
     * @var string|null
     */
    public $configVersion = null;

    /**
     * @var array<ConfigMountItemSpec>|null
     */
    public $mount = null;
}
