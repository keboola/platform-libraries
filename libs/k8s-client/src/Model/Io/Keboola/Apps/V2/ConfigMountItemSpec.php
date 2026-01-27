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
     * Container is the name of the container to mount the config in.
     *
     * Deprecated: In v2, container field is ignored as there is only one container
     * defined in containerSpec.
     */
    public string|null $container = null;

    public string|null $path = null;

    /** @var array<MountConfigField>|null */
    public array|null $fields = null;
}
