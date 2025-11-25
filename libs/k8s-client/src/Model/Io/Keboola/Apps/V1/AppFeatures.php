<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppFeatures defines optional features that can be enabled for the app
 *
 * @property StorageTokenSpec|null $storageToken
 * @property AppsProxyIngressSpec|null $appsProxyIngress
 * @property DataDirSpec|null $dataDir
 * @property ConfigMountSpec|null $mountConfig
 */
class AppFeatures extends AbstractModel
{
    /**
     * StorageToken defines configuration for the storage token
     */
    public StorageTokenSpec|null $storageToken = null;

    /**
     * AppsProxyIngress defines configuration for the apps proxy ingress feature
     */
    public AppsProxyIngressSpec|null $appsProxyIngress = null;

    /**
     * DataDir defines configuration for the data directory feature
     */
    public DataDirSpec|null $dataDir = null;

    /**
     * MountConfig defines configuration for mounting config files
     */
    public ConfigMountSpec|null $mountConfig = null;
}
