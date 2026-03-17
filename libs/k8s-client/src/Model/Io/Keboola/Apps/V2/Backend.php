<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

class Backend extends AbstractModel
{
    /** @var "k8sDeployment"|"e2bSandbox"|null  */
    public ?string $type = null;

    public ?E2bSandboxRuntime $e2bSandbox = null;
}
