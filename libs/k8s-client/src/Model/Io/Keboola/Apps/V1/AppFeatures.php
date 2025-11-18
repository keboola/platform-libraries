<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppFeatures defines the features configuration for an App
 *
 * @property array<string, mixed>|null $storageToken
 * @property array<string, mixed>|null $appsProxyIngress
 * @property array<string, mixed>|null $dataDir
 * @property array<string, mixed>|null $mountConfig
 */
class AppFeatures extends AbstractModel
{
    /**
     * StorageToken defines configuration for the storage token
     *
     * Structure:
     * - description: string - Human-readable description of the token's purpose
     * - canManageBuckets: bool - Indicates if the token can manage buckets
     * - canReadAllFileUploads: bool - Indicates if the token can read all file uploads
     * - canPurgeTrash: bool - Indicates if the token can purge trash
     * - bucketPermissions: array<string, string> - Maps bucket IDs to permission levels
     * - componentAccess: string[] - Specifies which components the token can access
     * - expiresIn: int - Duration in seconds after which the token expires (default: 86400, max: 31536000)
     * - setEnvs: array of objects with 'container' and 'envName'
     * - mountPaths: array of objects with 'container' and 'path'
     *
     * @var array<string, mixed>|null
     */
    public array|null $storageToken = null;

    /**
     * AppsProxyIngress defines configuration for the apps proxy ingress feature
     *
     * Structure:
     * - container: string (required) - Name of the container whose port should be exposed
     * - targetPort: int (required, default: 8888) - Port configuration for the app service (1-65535)
     *
     * @var array<string, mixed>|null
     */
    public array|null $appsProxyIngress = null;

    /**
     * DataDir defines configuration for the data directory feature
     *
     * Structure:
     * - mount: array of objects:
     *   - container: string (required) - Name of container to mount data directory in
     *   - path: string (default: /data) - Mount path for data directory in container
     * - dataLoader: object:
     *   - branchId: string (required) - ID of the branch this app belongs to
     *   - componentId: string (required) - ID of the component this app is based on
     *   - configId: string (required) - Unique identifier of the app configuration
     *   - port: int (default: 8080) - Port the data loader will listen on (1-65535)
     *
     * @var array<string, mixed>|null
     */
    public array|null $dataDir = null;

    /**
     * MountConfig defines configuration for mounting config files
     *
     * Structure:
     * - branchId: string (required) - ID of the branch to fetch the config from
     * - componentId: string (required) - ID of the component to fetch the config from
     * - configId: string (required) - ID of the config to fetch
     * - configVersion: string (optional) - Version of the config to fetch
     * - mount: array of objects (min 1):
     *   - container: string (required) - Name of container to mount config in
     *   - path: string (default: /data/config.json) - Mount path for config file in container
     *   - fields: array of objects (min 1):
     *     - source: string (required) - Source field name
     *     - target: string (required) - Target field name
     *
     * @var array<string, mixed>|null
     */
    public array|null $mountConfig = null;
}
