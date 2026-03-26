<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * ConfigMountItemSpec defines a mount configuration for a specific container
 */
class ConfigMountItemSpec extends AbstractModel
{
    /**
     * @var string
     */
    public $path = null;

    /**
     * @var MountConfigField[]
     */
    public $fields = null;
}
