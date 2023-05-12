<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\IngressesApiClient;
use Kubernetes\API\Ingress as IngressesApi;
use Kubernetes\Model\Io\K8s\Api\Networking\V1\Ingress;
use PHPUnit\Framework\TestCase;

class IngressesApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<IngressesApi, IngressesApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            IngressesApi::class,
            IngressesApiClient::class,
        );
    }

    private function getExcludedItemNamesFromCleanup(): array
    {
        return [];
    }

    protected function createResource(array $metadata): Ingress
    {
        return new Ingress([
            'metadata' => $metadata,
            'spec' => [
                'rules' => [
                    [
                        'host' => 'dummy.com',
                        'http' => [
                            'paths' => [
                                [
                                    'path' => '/',
                                    'pathType' => 'Prefix',
                                    'backend' => [
                                        'service' => [
                                            'name' => 'dummy-web-server',
                                            'port' => [
                                                'name' => 'http',
                                            ],
                                        ],
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]);
    }
}
