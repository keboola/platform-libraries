<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * AppsProxyIngressSpec defines configuration for the apps proxy ingress feature
 */
class AppsProxyIngressSpec extends AbstractModel
{
    /**
     * Container is the name of the container to expose via ingress.
     *
     * Deprecated: In v2, container field is ignored as there is only one container
     * defined in containerSpec.
     */
    public string|null $container = null;

    public int|null $targetPort = null;
}
