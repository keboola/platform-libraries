<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * ConfigMountSpec defines configuration for mounting config files
 */
class ConfigMountSpec extends AbstractModel
{
    public string|null $branchId = null;
    public string|null $componentId = null;
    public string|null $configId = null;
    public string|null $configVersion = null;

    /** @var array<ConfigMountItemSpec>|null */
    public array|null $mount = null;
}
