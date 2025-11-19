<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\AppsApiClient;
use Keboola\K8sClient\BaseApi\App as AppsApi;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppSpec;
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
                'appId' => $metadata['name'] ?? 'test-app',
                'configId' => 'test-config-123',
                'projectId' => 'test-project-456',
                'state' => 'Running',
                'replicas' => 1,
                'podSpec' => [
                    'containers' => [
                        [
                            'name' => 'main',
                            'image' => 'alpine:latest',
                        ],
                    ],
                ],
            ],
        ]);
    }

    public function testCreateOrPatchCreatesNewApp(): void
    {
        $app = $this->createResource([
            'name' => 'test-resource-1',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);

        $result = $this->apiClient->createOrPatch($app);

        self::assertNotNull($result->metadata);
        self::assertSame('test-resource-1', $result->metadata->name);
        self::assertNotNull($result->spec);
        self::assertSame('Running', $result->spec->state);
    }

    public function testCreateOrPatchUpdatesExistingApp(): void
    {
        // Create initial app
        $app = $this->createResource([
            'name' => 'test-resource-1',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);
        $this->apiClient->create($app);

        // Update the app using createOrPatch
        $app->spec = new AppSpec([
            'appId' => 'test-resource-1',
            'configId' => 'updated-config-456',
            'projectId' => 'test-project-456',
            'state' => 'Stopped',
            'replicas' => 1,
            'podSpec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'alpine:latest',
                    ],
                ],
            ],
        ]);

        $result = $this->apiClient->createOrPatch($app);

        self::assertNotNull($result->metadata);
        self::assertSame('test-resource-1', $result->metadata->name);
        self::assertNotNull($result->spec);
        self::assertSame('Stopped', $result->spec->state);
    }
}
