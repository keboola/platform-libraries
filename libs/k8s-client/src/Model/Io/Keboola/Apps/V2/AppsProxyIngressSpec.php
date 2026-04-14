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
     * Container is the name of the container whose port should be exposed
     *
     * @var string
     */
    public $container = null;

    /**
     * @var integer
     */
    public $targetPort = null;

    /**
     * Slug is an optional URL prefix used when building the public app URL.
     * When set, the URL is https://{slug}-{appId}.{hostnameSuffix}
     * When empty, the URL is https://{appId}.{hostnameSuffix}
     *
     * @var string|null
     */
    public $slug = null;
}
