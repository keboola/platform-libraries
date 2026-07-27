<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests;

use Keboola\K8sClient\ApiClient\ApiClientInterface;
use Keboola\K8sClient\ClientFactory\StaticKubernetesApiClientFactory;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;

class KubernetesApiClientFacadeCreateTest extends TestCase
{
    public function testCreateBuildsFacadeFromApiClient(): void
    {
        $facade = KubernetesApiClientFacade::create($this->apiClient(), new Logger('test'));

        self::assertInstanceOf(KubernetesApiClientFacade::class, $facade);
    }

    public function testCreateThreadsExtraClientsThroughToFacade(): void
    {
        $extraClient = $this->createMock(ApiClientInterface::class);

        $facade = KubernetesApiClientFacade::create(
            $this->apiClient(),
            new Logger('test'),
            [FakeCrdModel::class => $extraClient],
        );

        self::assertSame($extraClient, $facade->client(FakeCrdModel::class));
    }

    public function testCreateThreadsExtraClientMergePatchRouting(): void
    {
        $model = new FakeCrdModel(['metadata' => ['name' => 'thing-1'], 'spec' => ['size' => 2]]);

        $extraClient = $this->createMock(ApiClientInterface::class);
        $extraClient->expects(self::once())
            ->method('patch')
            ->willReturnCallback(function (string $name, Patch $patch) use ($model) {
                self::assertSame('thing-1', $name);
                $data = $patch->getArrayCopy();
                self::assertSame('merge-patch', $data['patchOperation']);
                self::assertSame(2, $data['spec']['size']);
                return $model;
            });

        $facade = KubernetesApiClientFacade::create(
            $this->apiClient(),
            new Logger('test'),
            [FakeCrdModel::class => $extraClient],
        );

        self::assertSame($model, $facade->mergePatch($model));
    }

    /**
     * Build a client through a real factory so the global KubernetesRuntime\Client is configured —
     * constructing the facade's core API clients requires it.
     */
    private function apiClient(): KubernetesApiClient
    {
        return (new StaticKubernetesApiClientFactory(
            new RetryProxy(),
            'https://example.test',
            'token',
            __DIR__ . '/fixtures/ca.crt',
            'my-namespace',
        ))->createApiClient();
    }
}
