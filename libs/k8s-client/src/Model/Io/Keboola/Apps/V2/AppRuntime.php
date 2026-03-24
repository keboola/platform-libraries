<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

class AppRuntime extends AbstractModel
{
    /**
     * RuntimeSize specifies the size of the runtime (defines resource allocation).
     * The actual resource values are configured via JSON file loaded from
     * RUNTIME_SIZES_CONFIG env variable. Fallback default is taken from
     * RUNTIME_SIZE_DEFAULT env variable if not specified.
     *
     * @var string
     */
    public $size = null;

    /**
     * @var Backend
     */
    public $backend = null;
}
