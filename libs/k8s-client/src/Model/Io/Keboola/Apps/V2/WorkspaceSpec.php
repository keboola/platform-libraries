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
     * @var string|null
     */
    public $configVersion = null;

    /**
     * BackendType defines the type of backend to use
     *
     * @var string|null
     */
    public $backend = null;

    /**
     * BackendSize defines the size of the backend
     *
     * @var string|null
     */
    public $backendSize = null;

    /**
     * LoginType defines the type of login to use
     *
     * @var string|null
     */
    public $loginType = null;

    /**
     * UseCase defines the use case for the workspace
     *
     * @var string|null
     */
    public $useCase = null;

    /**
     * NetworkPolicy defines the network policy to use
     *
     * @var string|null
     */
    public $networkPolicy = null;

    /**
     * PublicKey is the SSH public key to use for workspace access
     *
     * @var string|null
     */
    public $publicKey = null;

    /**
     * ReadOnlyStorageAccess when true creates the workspace with read-only storage access.
     * When false (default), the workspace has write access to storage.
     *
     * @var boolean|null
     */
    public $readOnlyStorageAccess = null;
}
