<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Api\Core\V1\LocalObjectReference;
use KubernetesRuntime\AbstractModel;

/**
 * AppsProxyStatus defines the observed state of the apps proxy
 */
class AppsProxyStatus extends AbstractModel
{
    /**
     * ServiceRef contains a reference to the service used for apps proxy
     *
     * @var LocalObjectReference
     */
    public $serviceRef = null;

    /**
     * UpstreamUrl is the URL of the upstream service
     *
     * @var string
     */
    public $upstreamUrl = null;
}
