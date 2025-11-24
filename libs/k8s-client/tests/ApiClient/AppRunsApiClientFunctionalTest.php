<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\AppRunsApiClient;
use Keboola\K8sClient\BaseApi\AppRun as AppRunsApi;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use PHPUnit\Framework\TestCase;

class AppRunsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<AppRunsApi, AppRunsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            AppRunsApi::class,
            AppRunsApiClient::class,
        );
    }

    protected function createResource(array $metadata): AppRun
    {
        return new AppRun([
            'metadata' => $metadata,
            'spec' => [
                'podRef' => [
                    'name' => 'app-12345-deployment-abc123-xyz',
                    'uid' => '550e8400-e29b-41d4-a716-446655440000',
                ],
                'appRef' => [
                    'name' => 'app-12345',
                    'appId' => 'app-123',
                    'projectId' => 'project-456',
                ],
                'createdAt' => '2025-01-15T12:00:00Z',
                'startedAt' => '2025-01-15T12:01:00Z',
                'state' => 'Running',
            ],
        ]);
    }

    public function testCreateAppRunWithFullSpec(): void
    {
        $appRun = $this->createResource([
            'name' => 'test-create-apprun',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);

        $result = $this->apiClient->create($appRun);

        // Metadata
        self::assertNotNull($result->metadata);
        self::assertSame('test-create-apprun', $result->metadata->name);

        // Spec basics
        self::assertNotNull($result->spec);
        self::assertSame('2025-01-15T12:00:00Z', $result->spec->createdAt);
        self::assertSame('2025-01-15T12:01:00Z', $result->spec->startedAt);
        self::assertSame('Running', $result->spec->state);

        // PodRef
        self::assertNotNull($result->spec->podRef);
        self::assertSame('app-12345-deployment-abc123-xyz', $result->spec->podRef->name);
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $result->spec->podRef->uid);

        // AppRef
        self::assertNotNull($result->spec->appRef);
        self::assertSame('app-12345', $result->spec->appRef->name);
        self::assertSame('app-123', $result->spec->appRef->appId);
        self::assertSame('project-456', $result->spec->appRef->projectId);
    }

    public function testPatchAppRunSpec(): void
    {
        // Create initial apprun
        $appRun = $this->createResource([
            'name' => 'test-patch-apprun',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);
        $this->apiClient->create($appRun);

        // Patch the spec
        $patch = new Patch([
            'spec' => [
                'state' => 'Finished',
                'stoppedAt' => '2025-01-15T13:00:00Z',
            ],
        ]);

        $result = $this->apiClient->patch('test-patch-apprun', $patch);

        self::assertNotNull($result->metadata);
        self::assertSame('test-patch-apprun', $result->metadata->name);
        self::assertNotNull($result->spec);
        self::assertSame('Finished', $result->spec->state);
        self::assertSame('2025-01-15T13:00:00Z', $result->spec->stoppedAt);
    }
}
