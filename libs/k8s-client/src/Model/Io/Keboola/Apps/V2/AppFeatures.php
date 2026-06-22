<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * AppFeatures defines optional features that can be enabled for the app
 */
class AppFeatures extends AbstractModel
{
    /**
     * StorageToken defines configuration for the storage token
     *
     * @var StorageTokenSpec
     */
    public $storageToken = null;

    /**
     * AppsProxyIngress defines configuration for the apps proxy ingress feature
     *
     * @var AppsProxyIngressSpec
     */
    public $appsProxyIngress = null;

    /**
     * DataDir defines configuration for the data directory feature
     *
     * @var DataDirSpec
     */
    public $dataDir = null;

    /**
     * MountConfig defines configuration for mounting config files
     *
     * @var ConfigMountSpec
     */
    public $mountConfig = null;

    /**
     * Workspace defines configuration for the workspace feature
     *
     * @var WorkspaceSpec
     */
    public $workspace = null;

    /**
     * ManagedGitRepo, when set, signals that the operator should mint per-AppRun
     * ephemeral git credentials (HTTP token or SSH key) and overlay them into the
     * rendered config.json Secret. Requires mountConfig to be set.
     *
     * @var ManagedGitRepoSpec|null
     */
    public $managedGitRepo = null;
}
