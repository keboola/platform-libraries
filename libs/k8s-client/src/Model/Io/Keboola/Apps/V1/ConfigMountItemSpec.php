<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * ConfigMountItemSpec defines a mount configuration for a specific container
 */
class ConfigMountItemSpec extends AbstractModel
{
    /**
     * @var string
     */
    public $container = null;

    /**
     * @var string
     */
    public $path = null;

    /**
     * @var array<MountConfigField>
     */
    public $fields = null;
}
