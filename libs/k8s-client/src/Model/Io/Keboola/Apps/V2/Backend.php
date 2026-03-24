<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

class Backend extends AbstractModel
{
    /**
     * @var string
     */
    public $type = null;

    /**
     * @var E2bSandboxRuntime
     */
    public $e2bSandbox = null;
}
