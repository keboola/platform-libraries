<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppReference contains information to identify the App resource
 *
 * @property string|null $name
 * @property string|null $appId
 * @property string|null $projectId
 */
class AppReference extends AbstractModel
{
    /**
     * Name is the name of the App resource
     */
    public string|null $name = null;

    /**
     * AppID is the appId from the App spec (for easier querying)
     */
    public string|null $appId = null;

    /**
     * ProjectID is the projectId from the App spec (for easier querying)
     */
    public string|null $projectId = null;
}
