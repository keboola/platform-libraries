<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\PodsApiClient;
use Kubernetes\API\Pod as PodsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use PHPUnit\Framework\TestCase;

class PodsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<PodsApi, PodsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            PodsApi::class,
            PodsApiClient::class,
        );
    }

    protected function createResource(array $metadata): Pod
    {
        return new Pod([
            'metadata' => $metadata,
            'spec' => [
                'containers' => [
                    [
                        'name' => 'main',
                        'image' => 'nginx',
                    ],
                ],
            ],
        ]);
    }

    private function getExcludedItemNamesFromCleanup(): array
    {
        return [];
    }
}
