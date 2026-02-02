<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\AppRunsApiClient;
use Keboola\K8sClient\BaseApi\AppRun as AppRunsApi;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
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
}
