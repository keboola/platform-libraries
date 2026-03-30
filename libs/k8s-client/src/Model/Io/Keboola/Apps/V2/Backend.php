<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

class Backend extends AbstractModel
{
    /**
     * Type selects the backend. Defaults to "k8sDeployment".
     * Possible values: k8sDeployment, e2bSandbox
     *
     * @var string
     */
    public $type = null;
}
