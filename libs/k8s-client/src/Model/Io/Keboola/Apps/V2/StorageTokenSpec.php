<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * StorageTokenSpec defines storage token configuration for an App
 */
class StorageTokenSpec extends AbstractModel
{
    /**
     * @var string|null
     */
    public $description = null;

    /**
     * @var integer|null
     */
    public $expiresIn = null;

    /**
     * @var string[]|null
     */
    public $componentAccess = null;

    /**
     * @var array<string, string>|null
     */
    public $bucketPermissions = null;

    /**
     * @var boolean|null
     */
    public $canReadAllFileUploads = null;

    /**
     * @var boolean|null
     */
    public $canPurgeTrash = null;

    /**
     * @var boolean|null
     */
    public $canManageBuckets = null;

    /**
     * @var SetEnvSpec[]|null
     */
    public $setEnvs = null;

    /**
     * @var MountPathSpec[]|null
     */
    public $mountPaths = null;
}
