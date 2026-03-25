<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * WorkspaceSpec defines configuration for the workspace feature
 */
class WorkspaceSpec extends AbstractModel
{
    /**
     * BackendType defines the type of backend to use
     *
     * @var string
     */
    public $backend = null;

    /**
     * BackendSize defines the size of the backend
     *
     * @var string
     */
    public $backendSize = null;

    /**
     * BranchID is the ID of the branch to fetch the config from
     *
     * @var string
     */
    public $branchId = null;

    /**
     * ComponentID is the ID of the component to fetch the config from
     *
     * @var string
     */
    public $componentId = null;

    /**
     * ConfigID is the ID of the config to fetch
     *
     * @var string
     */
    public $configId = null;

    /**
     * ConfigVersion is the version of the config to fetch (optional)
     *
     * @var string
     */
    public $configVersion = null;

    /**
     * LoginType defines the type of login to use
     *
     * @var string
     */
    public $loginType = null;

    /**
     * NetworkPolicy defines the network policy to use
     *
     * @var string
     */
    public $networkPolicy = null;
}
