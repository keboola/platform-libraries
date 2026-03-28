<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\Model\V2;

use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppFeatures;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppRuntime;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppsProxyIngressSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppsProxyStatus;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppStatus;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\Backend;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\ConfigMountItemSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\ConfigMountSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\ContainerSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\DataDirMountSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\DataDirSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\DataLoaderSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\E2bSandboxRuntime;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\E2bSandboxStatus;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\MountConfigField;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\MountPathSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\SetEnvSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\StorageTokenSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\WorkspaceSpec;
use Kubernetes\Model\Io\K8s\Api\Core\V1\EnvVar;
use Kubernetes\Model\Io\K8s\Api\Core\V1\HTTPGetAction;
use Kubernetes\Model\Io\K8s\Api\Core\V1\LocalObjectReference;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Probe;
use PHPUnit\Framework\TestCase;

class AppModelTest extends TestCase
{
    /**
     * Real-life test data based on V2 App structure
     * Key differences from V1:
     * - apiVersion is v2
     * - Uses containerSpec instead of podSpec
     * - ContainerSpec has no 'name' or 'resources' fields
     * - Added runtime field for resource allocation and backend configuration
     * - Deprecated 'container' fields still present in features
     */
    private static function getAppTestData(): array
    {
        return [
            'apiVersion' => 'apps.keboola.com/v2',
            'kind' => 'App',
            'metadata' => [
                'name' => 'app-12345',
            ],
            'spec' => [
                'appId' => '12345',
                'projectId' => 'project-789',
                'state' => 'Running',
                'replicas' => 1,
                'autoRestartEnabled' => false,
                'restartRequestedAt' => '2024-01-15T10:30:00Z',
                'runtimeSize' => 'small',
                'runtime' => [
                    'size' => 'small',
                    'backend' => [
                        'type' => 'e2bSandbox',
                        'e2bSandbox' => [
                            'templateId' => 'tpl-abc123',
                            'timeout' => '3600',
                        ],
                    ],
                ],
                'features' => [
                    'storageToken' => [
                        'description' => '[_internal][app] App 12345',
                        'expiresIn' => 86400,
                        'componentAccess' => ['keboola.streamlit'],
                        'bucketPermissions' => ['in.c-main' => 'read'],
                        'canManageBuckets' => true,
                        'canReadAllFileUploads' => true,
                        'canPurgeTrash' => false,
                        'setEnvs' => [
                            [
                                'container' => 'app',
                                'envName' => 'KBC_TOKEN',
                            ],
                        ],
                        'mountPaths' => [
                            [
                                'container' => 'app',
                                'path' => '/tmp/token',
                            ],
                        ],
                    ],
                    'appsProxyIngress' => [
                        'container' => 'app',
                        'targetPort' => 8888,
                    ],
                    'dataDir' => [
                        'mount' => [
                            [
                                'container' => 'app',
                                'path' => '/data',
                            ],
                        ],
                        'dataLoader' => [
                            'branchId' => 'main',
                            'componentId' => 'keboola.streamlit',
                            'configId' => 'config-456',
                            'port' => 8080,
                        ],
                    ],
                    'mountConfig' => [
                        'branchId' => 'main',
                        'componentId' => 'keboola.streamlit',
                        'configId' => 'config-456',
                        'configVersion' => '3',
                        'mount' => [
                            [
                                'container' => 'app',
                                'path' => '/data/config.json',
                                'fields' => [
                                    [
                                        'source' => '$.parameters.packages',
                                        'target' => 'packages',
                                        'strategy' => 'replace',
                                    ],
                                    [
                                        'source' => '$.parameters.script',
                                        'target' => 'script',
                                    ],
                                    [
                                        'target' => 'staticValue',
                                        'value' => 'hello',
                                        'strategy' => 'fallback',
                                    ],
                                    [
                                        'source' => '$.storage.input',
                                        'target' => 'input',
                                    ],
                                ],
                            ],
                        ],
                    ],
                    'workspace' => [
                        'branchId' => 'main',
                        'componentId' => 'keboola.streamlit',
                        'configId' => 'config-456',
                        'backend' => 'snowflake',
                        'backendSize' => 'small',
                    ],
                ],
                'containerSpec' => [
                    'image' => 'keboola.azurecr.io/docker-python-streamlit:1.2.3',
                    'command' => ['/bin/sh', '-c', 'streamlit run app.py'],
                    'env' => [
                        ['name' => 'KBC_URL', 'value' => 'https://connection.keboola.com'],
                        ['name' => 'SANDBOX_ID', 'value' => '12345'],
                        ['name' => 'PROJECT_ID', 'value' => 'project-789'],
                        ['name' => 'ROOT_DIR', 'value' => ''],
                        ['name' => 'DATA_LOADER_API_URL', 'value' => 'localhost:8080'],
                        ['name' => 'IS_OPERATOR', 'value' => 'true'],
                    ],
                    'startupProbe' => [
                        'httpGet' => [
                            'path' => '/',
                            'port' => 8888,
                        ],
                        'initialDelaySeconds' => 1,
                        'periodSeconds' => 1,
                        'failureThreshold' => 120,
                    ],
                    'readinessProbe' => [
                        'httpGet' => [
                            'path' => '/',
                            'port' => 8888,
                        ],
                        'initialDelaySeconds' => 0,
                        'periodSeconds' => 10,
                    ],
                    'livenessProbe' => [
                        'httpGet' => [
                            'path' => '/health',
                            'port' => 8888,
                        ],
                        'initialDelaySeconds' => 0,
                        'periodSeconds' => 10,
                    ],
                ],
            ],
            'status' => [
                'observedGeneration' => 5,
                'currentState' => 'Running',
                'readyReplicas' => 1,
                'updatedReplicas' => 1,
                'lastStartedTime' => '2024-01-15T10:31:00Z',
                'runStartRequestedAt' => '2024-01-15T10:30:00Z',
                'storageTokenRef' => [
                    'name' => 'app-12345-token',
                ],
                'appsProxyServiceRef' => [
                    'name' => 'app-12345-proxy',
                ],
                'appsProxy' => [
                    'serviceRef' => [
                        'name' => 'app-12345-proxy-svc',
                    ],
                    'upstreamUrl' => 'http://app-12345.ns.svc:8888',
                ],
                'e2bSandbox' => [
                    'name' => 'e2b-sandbox-12345',
                    'sandboxID' => 'sb-abc-123',
                    'startupLaunchedAt' => '2024-01-15T10:30:30Z',
                    'startupProbeFailures' => 0,
                    'syncedFileHashes' => ['app.py' => 'abc123'],
                ],
                'conditions' => [
                    [
                        'type' => 'Ready',
                        'status' => 'True',
                        'lastTransitionTime' => '2024-01-15T10:31:00Z',
                        'reason' => 'AppRunning',
                        'message' => 'App is running',
                    ],
                ],
            ],
        ];
    }

    public function testStreamlitAppModelHydration(): void
    {
        $data = self::getAppTestData();
        $app = new App($data);

        // Basic metadata
        self::assertNotNull($app->metadata);
        self::assertSame('app-12345', $app->metadata->name);

        // API version
        self::assertSame('apps.keboola.com/v2', $app->apiVersion);
        self::assertSame('App', $app->kind);

        // Spec basics
        self::assertNotNull($app->spec);
        self::assertInstanceOf(AppSpec::class, $app->spec);
        self::assertSame('12345', $app->spec->appId);
        self::assertSame('project-789', $app->spec->projectId);
        self::assertSame('Running', $app->spec->state);
        self::assertSame(1, $app->spec->replicas);
        self::assertFalse($app->spec->autoRestartEnabled);
        self::assertSame('2024-01-15T10:30:00Z', $app->spec->restartRequestedAt);
        self::assertSame('small', $app->spec->runtimeSize);
        self::assertNotNull($app->spec->runtime);
        self::assertInstanceOf(AppRuntime::class, $app->spec->runtime);
        self::assertSame('small', $app->spec->runtime->size);
        self::assertNotNull($app->spec->runtime->backend);
        self::assertInstanceOf(Backend::class, $app->spec->runtime->backend);
        self::assertSame('e2bSandbox', $app->spec->runtime->backend->type);
        self::assertNotNull($app->spec->runtime->backend->e2bSandbox);
        self::assertInstanceOf(E2bSandboxRuntime::class, $app->spec->runtime->backend->e2bSandbox);
        self::assertSame('tpl-abc123', $app->spec->runtime->backend->e2bSandbox->templateId);
        self::assertSame('3600', $app->spec->runtime->backend->e2bSandbox->timeout);

        // Features
        self::assertNotNull($app->spec->features);
        self::assertInstanceOf(AppFeatures::class, $app->spec->features);
        $this->assertStorageTokenFeature($app->spec->features->storageToken);
        $this->assertAppsProxyIngressFeature($app->spec->features->appsProxyIngress);
        $this->assertDataDirFeature($app->spec->features->dataDir);
        $this->assertMountConfigFeature($app->spec->features->mountConfig);
        $this->assertWorkspaceFeature($app->spec->features->workspace);

        // ContainerSpec
        self::assertNotNull($app->spec->containerSpec);
        self::assertInstanceOf(ContainerSpec::class, $app->spec->containerSpec);
        $this->assertContainerSpec($app->spec->containerSpec);

        // Status
        self::assertNotNull($app->status);
        self::assertInstanceOf(AppStatus::class, $app->status);
        self::assertSame(5, $app->status->observedGeneration);
        self::assertSame('Running', $app->status->currentState);
        self::assertSame(1, $app->status->readyReplicas);
        self::assertSame(1, $app->status->updatedReplicas);
        self::assertSame('2024-01-15T10:31:00Z', $app->status->lastStartedTime);
        self::assertSame('2024-01-15T10:30:00Z', $app->status->runStartRequestedAt);

        // Status - storageTokenRef and appsProxyServiceRef
        self::assertNotNull($app->status->storageTokenRef);
        self::assertInstanceOf(LocalObjectReference::class, $app->status->storageTokenRef);
        self::assertSame('app-12345-token', $app->status->storageTokenRef->name);
        self::assertNotNull($app->status->appsProxyServiceRef);
        self::assertInstanceOf(LocalObjectReference::class, $app->status->appsProxyServiceRef);
        self::assertSame('app-12345-proxy', $app->status->appsProxyServiceRef->name);

        // Status - appsProxy
        self::assertNotNull($app->status->appsProxy);
        self::assertInstanceOf(AppsProxyStatus::class, $app->status->appsProxy);
        self::assertSame('http://app-12345.ns.svc:8888', $app->status->appsProxy->upstreamUrl);
        self::assertNotNull($app->status->appsProxy->serviceRef);
        self::assertInstanceOf(LocalObjectReference::class, $app->status->appsProxy->serviceRef);
        self::assertSame('app-12345-proxy-svc', $app->status->appsProxy->serviceRef->name);

        // Status - e2bSandbox
        self::assertNotNull($app->status->e2bSandbox);
        self::assertInstanceOf(E2bSandboxStatus::class, $app->status->e2bSandbox);
        self::assertSame('e2b-sandbox-12345', $app->status->e2bSandbox->name);
        self::assertSame('sb-abc-123', $app->status->e2bSandbox->sandboxID);
        self::assertSame('2024-01-15T10:30:30Z', $app->status->e2bSandbox->startupLaunchedAt);
        self::assertSame(0, $app->status->e2bSandbox->startupProbeFailures);
        self::assertSame(['app.py' => 'abc123'], $app->status->e2bSandbox->syncedFileHashes);

        // Status - conditions
        self::assertNotNull($app->status->conditions);
        self::assertCount(1, $app->status->conditions);
        self::assertSame('Ready', $app->status->conditions[0]->type);
        self::assertSame('True', $app->status->conditions[0]->status);
    }

    private function assertStorageTokenFeature(?StorageTokenSpec $storageToken): void
    {
        self::assertNotNull($storageToken);
        self::assertInstanceOf(StorageTokenSpec::class, $storageToken);
        self::assertSame('[_internal][app] App 12345', $storageToken->description);
        self::assertSame(86400, $storageToken->expiresIn);
        self::assertSame(['keboola.streamlit'], $storageToken->componentAccess);
        self::assertSame(['in.c-main' => 'read'], $storageToken->bucketPermissions);
        self::assertTrue($storageToken->canManageBuckets);
        self::assertTrue($storageToken->canReadAllFileUploads);
        self::assertFalse($storageToken->canPurgeTrash);

        // setEnvs
        self::assertNotNull($storageToken->setEnvs);
        self::assertCount(1, $storageToken->setEnvs);
        self::assertInstanceOf(SetEnvSpec::class, $storageToken->setEnvs[0]);
        self::assertSame('app', $storageToken->setEnvs[0]->container);
        self::assertSame('KBC_TOKEN', $storageToken->setEnvs[0]->envName);

        // mountPaths
        self::assertNotNull($storageToken->mountPaths);
        self::assertCount(1, $storageToken->mountPaths);
        self::assertInstanceOf(MountPathSpec::class, $storageToken->mountPaths[0]);
        self::assertSame('app', $storageToken->mountPaths[0]->container);
        self::assertSame('/tmp/token', $storageToken->mountPaths[0]->path);
    }

    private function assertAppsProxyIngressFeature(?AppsProxyIngressSpec $appsProxyIngress): void
    {
        self::assertNotNull($appsProxyIngress);
        self::assertInstanceOf(AppsProxyIngressSpec::class, $appsProxyIngress);
        self::assertSame('app', $appsProxyIngress->container);
        self::assertSame(8888, $appsProxyIngress->targetPort);
    }

    private function assertDataDirFeature(?DataDirSpec $dataDir): void
    {
        self::assertNotNull($dataDir);
        self::assertInstanceOf(DataDirSpec::class, $dataDir);

        // mount
        self::assertNotNull($dataDir->mount);
        self::assertCount(1, $dataDir->mount);
        self::assertInstanceOf(DataDirMountSpec::class, $dataDir->mount[0]);
        self::assertSame('app', $dataDir->mount[0]->container);
        self::assertSame('/data', $dataDir->mount[0]->path);

        // dataLoader
        self::assertNotNull($dataDir->dataLoader);
        self::assertInstanceOf(DataLoaderSpec::class, $dataDir->dataLoader);
        self::assertSame('main', $dataDir->dataLoader->branchId);
        self::assertSame('keboola.streamlit', $dataDir->dataLoader->componentId);
        self::assertSame('config-456', $dataDir->dataLoader->configId);
        self::assertSame(8080, $dataDir->dataLoader->port);
    }

    private function assertMountConfigFeature(?ConfigMountSpec $mountConfig): void
    {
        self::assertNotNull($mountConfig);
        self::assertInstanceOf(ConfigMountSpec::class, $mountConfig);
        self::assertSame('main', $mountConfig->branchId);
        self::assertSame('keboola.streamlit', $mountConfig->componentId);
        self::assertSame('config-456', $mountConfig->configId);
        self::assertSame('3', $mountConfig->configVersion);

        // mount
        self::assertNotNull($mountConfig->mount);
        self::assertCount(1, $mountConfig->mount);
        self::assertInstanceOf(ConfigMountItemSpec::class, $mountConfig->mount[0]);
        self::assertSame('app', $mountConfig->mount[0]->container);
        self::assertSame('/data/config.json', $mountConfig->mount[0]->path);

        // fields
        self::assertNotNull($mountConfig->mount[0]->fields);
        self::assertCount(4, $mountConfig->mount[0]->fields);

        // First field - with source and strategy
        self::assertInstanceOf(MountConfigField::class, $mountConfig->mount[0]->fields[0]);
        self::assertSame('$.parameters.packages', $mountConfig->mount[0]->fields[0]->source);
        self::assertSame('packages', $mountConfig->mount[0]->fields[0]->target);
        self::assertSame('replace', $mountConfig->mount[0]->fields[0]->strategy);

        // Third field - with static value and fallback strategy
        self::assertInstanceOf(MountConfigField::class, $mountConfig->mount[0]->fields[2]);
        self::assertNull($mountConfig->mount[0]->fields[2]->source);
        self::assertSame('staticValue', $mountConfig->mount[0]->fields[2]->target);
        self::assertSame('hello', $mountConfig->mount[0]->fields[2]->value);
        self::assertSame('fallback', $mountConfig->mount[0]->fields[2]->strategy);
    }

    private function assertWorkspaceFeature(?WorkspaceSpec $workspace): void
    {
        self::assertNotNull($workspace);
        self::assertInstanceOf(WorkspaceSpec::class, $workspace);
        self::assertSame('main', $workspace->branchId);
        self::assertSame('keboola.streamlit', $workspace->componentId);
        self::assertSame('config-456', $workspace->configId);
        self::assertSame('snowflake', $workspace->backend);
        self::assertSame('small', $workspace->backendSize);
    }

    private function assertContainerSpec(ContainerSpec $containerSpec): void
    {
        self::assertSame('keboola.azurecr.io/docker-python-streamlit:1.2.3', $containerSpec->image);

        // command
        self::assertNotNull($containerSpec->command);
        self::assertIsArray($containerSpec->command);
        self::assertCount(3, $containerSpec->command);
        self::assertSame(['/bin/sh', '-c', 'streamlit run app.py'], $containerSpec->command);

        // env vars
        self::assertNotNull($containerSpec->env);
        self::assertCount(6, $containerSpec->env);
        self::assertInstanceOf(EnvVar::class, $containerSpec->env[0]);
        self::assertSame('KBC_URL', $containerSpec->env[0]->name);
        self::assertSame('https://connection.keboola.com', $containerSpec->env[0]->value);

        // probes
        self::assertNotNull($containerSpec->startupProbe);
        self::assertInstanceOf(Probe::class, $containerSpec->startupProbe);
        self::assertNotNull($containerSpec->startupProbe->httpGet);
        self::assertInstanceOf(HTTPGetAction::class, $containerSpec->startupProbe->httpGet);
        self::assertSame('/', $containerSpec->startupProbe->httpGet->path);
        self::assertEquals(8888, $containerSpec->startupProbe->httpGet->port);
        self::assertSame(1, $containerSpec->startupProbe->initialDelaySeconds);
        self::assertSame(1, $containerSpec->startupProbe->periodSeconds);
        self::assertSame(120, $containerSpec->startupProbe->failureThreshold);

        self::assertNotNull($containerSpec->readinessProbe);
        self::assertInstanceOf(Probe::class, $containerSpec->readinessProbe);
        self::assertSame(0, $containerSpec->readinessProbe->initialDelaySeconds);
        self::assertSame(10, $containerSpec->readinessProbe->periodSeconds);

        self::assertNotNull($containerSpec->livenessProbe);
        self::assertInstanceOf(Probe::class, $containerSpec->livenessProbe);
        self::assertNotNull($containerSpec->livenessProbe->httpGet);
        self::assertSame('/health', $containerSpec->livenessProbe->httpGet->path);
    }

    public function testAppModelSerialization(): void
    {
        $data = self::getAppTestData();
        $app = new App($data);

        // Test that the model can be serialized back to array
        $serialized = $app->getArrayCopy();

        self::assertIsArray($serialized);
        self::assertArrayHasKey('apiVersion', $serialized);
        self::assertArrayHasKey('kind', $serialized);
        self::assertArrayHasKey('metadata', $serialized);
        self::assertArrayHasKey('spec', $serialized);

        // Verify API version
        self::assertSame('apps.keboola.com/v2', $serialized['apiVersion']);
        self::assertSame('App', $serialized['kind']);

        // Verify key nested values survive round-trip
        self::assertSame('12345', $serialized['spec']['appId']);
        self::assertSame('project-789', $serialized['spec']['projectId']);
        self::assertSame('Running', $serialized['spec']['state']);
        self::assertArrayHasKey('runtime', $serialized['spec']);
        self::assertSame('small', $serialized['spec']['runtime']['size']);
        self::assertSame('e2bSandbox', $serialized['spec']['runtime']['backend']['type']);
        self::assertSame('tpl-abc123', $serialized['spec']['runtime']['backend']['e2bSandbox']['templateId']);
        self::assertSame('small', $serialized['spec']['runtimeSize']);

        // Verify containerSpec is present and podSpec is not
        self::assertArrayHasKey('containerSpec', $serialized['spec']);
        self::assertArrayNotHasKey('podSpec', $serialized['spec']);

        // Verify containerSpec structure
        self::assertSame(
            'keboola.azurecr.io/docker-python-streamlit:1.2.3',
            $serialized['spec']['containerSpec']['image'],
        );
        self::assertIsArray($serialized['spec']['containerSpec']['command']);
        self::assertArrayNotHasKey('name', $serialized['spec']['containerSpec']);
        self::assertArrayNotHasKey('resources', $serialized['spec']['containerSpec']);
    }

    public function testCreateAppWithNestedObjects(): void
    {
        $data = self::getAppTestData();
        $app = new App($data);

        // Test containerSpec
        self::assertNotNull($app->spec);
        self::assertInstanceOf(ContainerSpec::class, $app->spec->containerSpec);
        self::assertNotNull($app->spec->containerSpec);
        self::assertNotNull($app->spec->containerSpec->env);
        self::assertCount(6, $app->spec->containerSpec->env);
        self::assertInstanceOf(EnvVar::class, $app->spec->containerSpec->env[0]);

        // Test probes
        self::assertInstanceOf(Probe::class, $app->spec->containerSpec->startupProbe);
        self::assertNotNull($app->spec->containerSpec->startupProbe);
        self::assertInstanceOf(HTTPGetAction::class, $app->spec->containerSpec->startupProbe->httpGet);
        self::assertInstanceOf(Probe::class, $app->spec->containerSpec->readinessProbe);
        self::assertInstanceOf(Probe::class, $app->spec->containerSpec->livenessProbe);

        // Test features
        self::assertNotNull($app->spec->features);
        self::assertInstanceOf(AppFeatures::class, $app->spec->features);
        self::assertInstanceOf(StorageTokenSpec::class, $app->spec->features->storageToken);
        self::assertInstanceOf(DataDirSpec::class, $app->spec->features->dataDir);
        self::assertInstanceOf(ConfigMountSpec::class, $app->spec->features->mountConfig);
        self::assertInstanceOf(WorkspaceSpec::class, $app->spec->features->workspace);

        // Test status
        self::assertNotNull($app->status);
        self::assertInstanceOf(AppStatus::class, $app->status);
        self::assertInstanceOf(AppsProxyStatus::class, $app->status->appsProxy);
        self::assertInstanceOf(E2bSandboxStatus::class, $app->status->e2bSandbox);
        self::assertInstanceOf(LocalObjectReference::class, $app->status->storageTokenRef);
        self::assertInstanceOf(LocalObjectReference::class, $app->status->appsProxyServiceRef);
    }
}
