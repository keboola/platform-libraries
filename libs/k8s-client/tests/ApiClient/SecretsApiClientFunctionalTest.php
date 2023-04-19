<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ApiClient;

use Keboola\K8sClient\ApiClient\SecretsApiClient;
use Kubernetes\API\Secret as SecretsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Secret;
use PHPUnit\Framework\TestCase;

class SecretsApiClientFunctionalTest extends TestCase
{
    /**
     * @template-use BaseNamespaceApiClientTestCase<SecretsApi, SecretsApiClient>
     */
    use BaseNamespaceApiClientTestCase;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpBaseNamespaceApiClientTest(
            SecretsApi::class,
            SecretsApiClient::class,
        );
    }

    protected function createResource(array $metadata): Secret
    {
        return new Secret([
            'metadata' => $metadata,
            'data' => [
                'test_key' => base64_encode('test_value'),
            ],
        ]);
    }
}
