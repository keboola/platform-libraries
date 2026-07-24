<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory;

use Keboola\K8sClient\ApiClient\ApiClientInterface;
use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\Token\StaticToken;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\Tests\FakeCrdModel;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;

class GenericClientFacadeFactoryTest extends TestCase
{
    public function testCreateApiClientReturnsNamespacedClient(): void
    {
        $factory = new GenericClientFacadeFactory(new RetryProxy(), new Logger('test'));

        $apiClient = $factory->createApiClient(
            'https://example.test',
            new StaticToken('token'),
            __DIR__ . '/../fixtures/ca.crt',
            'my-namespace',
        );

        self::assertInstanceOf(KubernetesApiClient::class, $apiClient);
        self::assertSame('my-namespace', $apiClient->getK8sNamespace());
    }

    public function testCreateClusterClientThreadsExtraClientsThroughToFacade(): void
    {
        $factory = new GenericClientFacadeFactory(new RetryProxy(), new Logger('test'));

        $extraClient = $this->createMock(ApiClientInterface::class);

        $facade = $factory->createClusterClient(
            'https://example.test',
            new StaticToken('token'),
            __DIR__ . '/../fixtures/ca.crt',
            'my-namespace',
            [FakeCrdModel::class => $extraClient],
        );

        self::assertSame($extraClient, $facade->client(FakeCrdModel::class));
    }
}
