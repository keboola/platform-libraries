<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * StorageTokenSpec defines storage token configuration for an App
 */
class StorageTokenSpec extends AbstractModel
{
    public string|null $description = null;
    public int|null $expiresIn = null;

    /** @var array<string>|null */
    public array|null $componentAccess = null;

    /** @var array<string, string>|null */
    public array|null $bucketPermissions = null;

    public bool|null $canReadAllFileUploads = null;
    public bool|null $canPurgeTrash = null;
    public bool|null $canManageBuckets = null;

    /** @var array<SetEnvSpec>|null */
    public array|null $setEnvs = null;

    /** @var array<MountPathSpec>|null */
    public array|null $mountPaths = null;
}
