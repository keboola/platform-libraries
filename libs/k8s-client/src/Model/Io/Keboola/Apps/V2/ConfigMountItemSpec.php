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
     * Container is the name of the container to mount the config in
     *
     * @var string
     */
    public $container = null;

    /**
     * Path is the mount path for the config file in the container
     *
     * @var string
     */
    public $path = null;

    /**
     * @var MountConfigField[]
     */
    public $fields = null;
}
