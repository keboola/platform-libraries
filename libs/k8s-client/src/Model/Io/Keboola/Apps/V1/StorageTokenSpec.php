<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

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
     * @var int|null
     */
    public $expiresIn = null;

    /**
     * @var array<string>|null
     */
    public $componentAccess = null;

    /**
     * @var array<string, string>|null
     */
    public $bucketPermissions = null;

    /**
     * @var bool|null
     */
    public $canReadAllFileUploads = null;

    /**
     * @var bool|null
     */
    public $canPurgeTrash = null;

    /**
     * @var bool|null
     */
    public $canManageBuckets = null;

    /**
     * @var array<SetEnvSpec>|null
     */
    public $setEnvs = null;

    /**
     * @var array<MountPathSpec>|null
     */
    public $mountPaths = null;
}
