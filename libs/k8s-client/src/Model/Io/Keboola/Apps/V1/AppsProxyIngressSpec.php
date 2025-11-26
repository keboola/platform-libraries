<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppsProxyIngressSpec defines configuration for the apps proxy ingress feature
 */
class AppsProxyIngressSpec extends AbstractModel
{
    /**
     * @var string|null
     */
    public $container = null;

    /**
     * @var int|null
     */
    public $targetPort = null;
}
