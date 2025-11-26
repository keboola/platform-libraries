<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * MountConfigField defines a field mapping in the config file
 */
class MountConfigField extends AbstractModel
{
    /**
     * @var string
     */
    public $source = null;

    /**
     * @var string
     */
    public $target = null;
}
