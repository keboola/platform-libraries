<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

class E2bSandboxRuntime extends AbstractModel
{
    public ?string $templateId = null;
    public ?string $timeout = null;
}
