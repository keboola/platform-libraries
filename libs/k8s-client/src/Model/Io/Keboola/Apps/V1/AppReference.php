<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppReference contains information to identify the App resource
 *
 * @property string $name
 * @property string $appId
 * @property string $projectId
 */
class AppReference extends AbstractModel
{
    /**
     * Name is the name of the App resource
     *
     * @var string
     */
    public $name = null;

    /**
     * AppID is the appId from the App spec (for easier querying)
     *
     * @var string
     */
    public $appId = null;

    /**
     * ProjectID is the projectId from the App spec (for easier querying)
     *
     * @var string
     */
    public $projectId = null;
}
