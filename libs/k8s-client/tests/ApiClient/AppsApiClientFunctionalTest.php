<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\AppsApiClient;
use Keboola\K8sClient\BaseApi\App as AppsApi;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\App;
use PHPUnit\Framework\TestCase;

class AppsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<AppsApi, AppsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            AppsApi::class,
            AppsApiClient::class,
        );
    }

    protected function createResource(array $metadata): App
    {
        return new App([
            'metadata' => $metadata,
            'spec' => [
                'appId' => 'app-123',
                'projectId' => 'project-456',
                'state' => 'Running',
                'replicas' => 1,
                'runtimeSize' => 'small',
                'features' => [
                    'storageToken' => [
                        'description' => 'test-token',
                        'canManageBuckets' => true,
                        'canReadAllFileUploads' => true,
                        'canPurgeTrash' => false,
                        'setEnvs' => [['envName' => 'KBC_TOKEN']],
                    ],
                    'appsProxyIngress' => [
                        'targetPort' => 8080,
                    ],
                    'dataDir' => [
                        'mount' => [[]],
                        'dataLoader' => [
                            'branchId' => 'main',
                            'componentId' => 'component-1',
                            'configId' => 'config-1',
                        ],
                    ],
                    'mountConfig' => [
                        'branchId' => 'main',
                        'componentId' => 'component-1',
                        'configId' => 'config-1',
                        'mount' => [[
                            'path' => '/config.json',
                            'fields' => [['source' => '$.foo', 'target' => 'bar']],
                        ]],
                    ],
                ],
                'containerSpec' => [
                    'image' => 'busybox',
                    'env' => [['name' => 'FOO', 'value' => 'bar']],
                    'startupProbe' => [
                        'httpGet' => ['path' => '/', 'port' => 8080],
                        'periodSeconds' => 1,
                        'failureThreshold' => 30,
                    ],
                    'readinessProbe' => [
                        'httpGet' => ['path' => '/', 'port' => 8080],
                        'periodSeconds' => 10,
                    ],
                ],
            ],
        ]);
    }
}
