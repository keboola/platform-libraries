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
                        'image' => 'alpine',
                        'command' => ['sh', '-c', 'echo "Hello World"'],
                    ],
                ],
            ],
        ]);
    }

    public function testReadLogWhenNodeSelectorIsUnsatisfied(): void
    {

        $pod = $this->createResource([
            'name' => 'test-resource-1',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);

        $pod->spec->nodeSelector = [
            'nodePool' => 'dummy-pool',
        ];

        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), $pod);
        self::assertSame('', $this->apiClient->readLog('test-resource-1'));
    }

    public function testReadLog(): void
    {

        $pod = $this->createResource([
            'name' => 'test-resource-1',
            'labels' => [
                'app' => 'test-1',
                self::getTestResourcesLabelName() => (string) getenv('K8S_NAMESPACE'),
            ],
        ]);

        $this->baseApiClient->create((string) getenv('K8S_NAMESPACE'), $pod);
        sleep(5);
        $log = $this->apiClient->readLog('test-resource-1');
        self::assertStringContainsString('Hello World', $log);
    }
}
