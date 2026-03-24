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
     * @var string
     */
    public $description = null;

    /**
     * @var integer
     */
    public $expiresIn = null;

    /**
     * @var string[]
     */
    public $componentAccess = null;

    /**
     * @var array
     */
    public $bucketPermissions = null;

    /**
     * @var boolean
     */
    public $canReadAllFileUploads = null;

    /**
     * @var boolean
     */
    public $canPurgeTrash = null;

    /**
     * @var boolean
     */
    public $canManageBuckets = null;

    /**
     * @var SetEnvSpec[]
     */
    public $setEnvs = null;

    /**
     * @var MountPathSpec[]
     */
    public $mountPaths = null;
}
