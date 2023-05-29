<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\ConfigMapsApiClient;
use Kubernetes\API\ConfigMap as ConfigMapsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\ConfigMap;
use PHPUnit\Framework\TestCase;

class ConfigMapsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<ConfigMapsApi, ConfigMapsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            ConfigMapsApi::class,
            ConfigMapsApiClient::class
        );
    }

    protected function createResource(array $metadata): ConfigMap
    {
        return new ConfigMap([
            'metadata' => $metadata,
            'data' => [
                'test_key' => 'test_value',
            ],
        ]);
    }
}
