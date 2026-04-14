<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Api\Core\V1\LocalObjectReference;
use KubernetesRuntime\AbstractModel;

/**
 * AppsProxyStatus holds status for the apps proxy ingress feature
 */
class AppsProxyStatus extends AbstractModel
{
    /**
     * ServiceRef contains a reference to the k8s Service used for apps proxy ingress.
     * Not set for e2bSandbox backends (no service is created).
     *
     * @var LocalObjectReference|null
     */
    public $serviceRef = null;

    /**
     * UpstreamUrl is the URL of the upstream app service.
     *
     * @var string|null
     */
    public $upstreamUrl = null;

    /**
     * PublicUrl is the URL at which the app is exposed publicly via the Apps Proxy.
     * Computed from spec.features.appsProxyIngress.slug, spec.appId, and the operator's
     * HostnameSuffix. Empty when AppsProxyIngress is not enabled.
     *
     * @var string|null
     */
    public $publicUrl = null;
}
