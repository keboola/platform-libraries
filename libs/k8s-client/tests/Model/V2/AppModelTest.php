<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\Model\V2;

use Generator;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppFeatures;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppsProxyIngressSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\ConfigMountItemSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\ConfigMountSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\ContainerSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\DataDirMountSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\DataDirSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\DataLoaderSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\MountConfigField;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\SetEnvSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\StorageTokenSpec;
use Kubernetes\Model\Io\K8s\Api\Core\V1\EnvVar;
use Kubernetes\Model\Io\K8s\Api\Core\V1\HTTPGetAction;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Probe;
use PHPUnit\Framework\TestCase;
use TypeError;

class AppModelTest extends TestCase
{
    /**
     * Real-life test data based on V2 App structure
     * Key differences from V1:
     * - apiVersion is v2
     * - Uses containerSpec instead of podSpec
     * - ContainerSpec has no 'name' or 'resources' fields
     * - Added runtimeSize field for resource allocation
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
                'features' => [
                    'storageToken' => [
                        'description' => '[_internal][app] App 12345',
                        'canManageBuckets' => true,
                        'canReadAllFileUploads' => true,
                        'canPurgeTrash' => false,
                        'setEnvs' => [
                            [
                                'envName' => 'KBC_TOKEN',
                            ],
                        ],
                    ],
                    'appsProxyIngress' => [
                        'targetPort' => 8888,
                    ],
                    'dataDir' => [
                        'mount' => [
                            [
                                'path' => '/data',
                            ],
                        ],
                        'dataLoader' => [
                            'branchId' => 'main',
                            'componentId' => 'keboola.streamlit',
                            'configId' => 'config-456',
                        ],
                    ],
                    'mountConfig' => [
                        'branchId' => 'main',
                        'componentId' => 'keboola.streamlit',
                        'configId' => 'config-456',
                        'configVersion' => '3',
                        'mount' => [
                            [
                                'path' => '/data/config.json',
                                'fields' => [
                                    [
                                        'source' => '$.parameters.packages',
                                        'target' => 'packages',
                                    ],
                                    [
                                        'source' => '$.parameters.script',
                                        'target' => 'script',
                                    ],
                                    [
                                        'source' => '$.parameters.dataApp',
                                        'target' => 'dataApp',
                                    ],
                                    [
                                        'source' => '$.storage.input',
                                        'target' => 'input',
                                    ],
                                ],
                            ],
                        ],
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

        // Features
        self::assertNotNull($app->spec->features);
        self::assertInstanceOf(AppFeatures::class, $app->spec->features);
        $this->assertStorageTokenFeature($app->spec->features->storageToken);
        $this->assertAppsProxyIngressFeature($app->spec->features->appsProxyIngress);
        $this->assertDataDirFeature($app->spec->features->dataDir);
        $this->assertMountConfigFeature($app->spec->features->mountConfig);

        // ContainerSpec
        self::assertNotNull($app->spec->containerSpec);
        self::assertInstanceOf(ContainerSpec::class, $app->spec->containerSpec);
        $this->assertContainerSpec($app->spec->containerSpec);
    }

    private function assertStorageTokenFeature(?StorageTokenSpec $storageToken): void
    {
        self::assertNotNull($storageToken);
        self::assertInstanceOf(StorageTokenSpec::class, $storageToken);
        self::assertSame('[_internal][app] App 12345', $storageToken->description);
        self::assertTrue($storageToken->canManageBuckets);
        self::assertTrue($storageToken->canReadAllFileUploads);
        self::assertFalse($storageToken->canPurgeTrash);

        // setEnvs
        self::assertNotNull($storageToken->setEnvs);
        self::assertCount(1, $storageToken->setEnvs);
        self::assertInstanceOf(SetEnvSpec::class, $storageToken->setEnvs[0]);
        self::assertSame('KBC_TOKEN', $storageToken->setEnvs[0]->envName);
    }

    private function assertAppsProxyIngressFeature(?AppsProxyIngressSpec $appsProxyIngress): void
    {
        self::assertNotNull($appsProxyIngress);
        self::assertInstanceOf(AppsProxyIngressSpec::class, $appsProxyIngress);
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

        // dataLoader
        self::assertNotNull($dataDir->dataLoader);
        self::assertInstanceOf(DataLoaderSpec::class, $dataDir->dataLoader);
        self::assertSame('main', $dataDir->dataLoader->branchId);
        self::assertSame('keboola.streamlit', $dataDir->dataLoader->componentId);
        self::assertSame('config-456', $dataDir->dataLoader->configId);
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
        self::assertSame('/data/config.json', $mountConfig->mount[0]->path);

        // fields
        self::assertNotNull($mountConfig->mount[0]->fields);
        self::assertCount(4, $mountConfig->mount[0]->fields);

        $expectedFields = [
            ['$.parameters.packages', 'packages'],
            ['$.parameters.script', 'script'],
            ['$.parameters.dataApp', 'dataApp'],
            ['$.storage.input', 'input'],
        ];

        foreach ($mountConfig->mount[0]->fields as $index => $field) {
            self::assertInstanceOf(MountConfigField::class, $field);
            self::assertSame($expectedFields[$index][0], $field->source);
            self::assertSame($expectedFields[$index][1], $field->target);
        }
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
    }

    public static function provideInvalidTestData(): Generator
    {
        yield 'wrong replicas type - string instead of int' => [
            'data' => ['spec' => ['replicas' => 'one']],
            'expectedMessage' => 'Cannot assign string to property',
        ];

        yield 'wrong targetPort type - string instead of int' => [
            'data' => ['spec' => ['features' => ['appsProxyIngress' => ['targetPort' => 'invalid']]]],
            'expectedMessage' => 'Cannot assign string to property',
        ];

        yield 'wrong canManageBuckets type - array instead of bool' => [
            'data' => ['spec' => ['features' => ['storageToken' => ['canManageBuckets' => []]]]],
            'expectedMessage' => 'Cannot assign array to property',
        ];

        yield 'wrong observedGeneration type - string instead of int' => [
            'data' => ['status' => ['observedGeneration' => 'invalid']],
            'expectedMessage' => 'Cannot assign string to property',
        ];

        yield 'wrong autoRestartEnabled type - string instead of bool' => [
            'data' => ['spec' => ['autoRestartEnabled' => []]],
            'expectedMessage' => 'Cannot assign array to property',
        ];

        yield 'wrong restartRequestedAt type - array instead of string' => [
            'data' => ['spec' => ['restartRequestedAt' => []]],
            'expectedMessage' => 'Cannot assign array to property',
        ];
    }

    /**
     * @dataProvider provideInvalidTestData
     */
    public function testInvalidDataTypesThrowTypeError(array $data, string $expectedMessage): void
    {
        $this->expectException(TypeError::class);
        $this->expectExceptionMessageMatches('/' . preg_quote($expectedMessage, '/') . '/');

        new App($data);
    }
}
