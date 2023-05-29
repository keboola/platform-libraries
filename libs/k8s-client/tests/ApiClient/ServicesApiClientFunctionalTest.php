<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\ServicesApiClient;
use Kubernetes\API\Service as ServicesApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Service;
use PHPUnit\Framework\TestCase;

class ServicesApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<ServicesApi, ServicesApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            ServicesApi::class,
            ServicesApiClient::class,
        );
    }

    protected function createResource(array $metadata): Service
    {
        return new Service([
            'metadata' => $metadata,
            'spec' => [
                'selector' => [
                    'app' => 'ServicesApiClientFunctionalTest',
                ],
                'ports' => [
                    [
                        'name' => 'test-port',
                        'port' => 1234,
                    ],
                ],
            ],
        ]);
    }
}
