<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFactory;

use InvalidArgumentException;
use Keboola\K8sClient\ClientFactory\StaticKubernetesApiClientFactory;
use Keboola\K8sClient\ClientFactory\Token\StaticToken;
use Keboola\K8sClient\KubernetesApiClient;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;

class StaticKubernetesApiClientFactoryTest extends TestCase
{
    public function testCreateApiClientReturnsNamespacedClient(): void
    {
        $factory = new StaticKubernetesApiClientFactory(
            new RetryProxy(),
            'https://example.test',
            new StaticToken('token'),
            __DIR__ . '/../fixtures/ca.crt',
        );

        $apiClient = $factory->createApiClient('my-namespace');

        self::assertInstanceOf(KubernetesApiClient::class, $apiClient);
        self::assertSame('my-namespace', $apiClient->getK8sNamespace());
    }

    public function testCreateApiClientUsesDefaultNamespaceWhenNoneProvided(): void
    {
        $factory = new StaticKubernetesApiClientFactory(
            new RetryProxy(),
            'https://example.test',
            new StaticToken('token'),
            __DIR__ . '/../fixtures/ca.crt',
            'default-namespace',
        );

        $apiClient = $factory->createApiClient();

        self::assertSame('default-namespace', $apiClient->getK8sNamespace());
    }

    public function testCreateApiClientArgumentOverridesDefaultNamespace(): void
    {
        $factory = new StaticKubernetesApiClientFactory(
            new RetryProxy(),
            'https://example.test',
            new StaticToken('token'),
            __DIR__ . '/../fixtures/ca.crt',
            'default-namespace',
        );

        $apiClient = $factory->createApiClient('explicit-namespace');

        self::assertSame('explicit-namespace', $apiClient->getK8sNamespace());
    }

    public function testCreateApiClientAcceptsRawStringToken(): void
    {
        $factory = new StaticKubernetesApiClientFactory(
            new RetryProxy(),
            'https://example.test',
            'raw-token',
            __DIR__ . '/../fixtures/ca.crt',
            'my-namespace',
        );

        $apiClient = $factory->createApiClient();

        self::assertInstanceOf(KubernetesApiClient::class, $apiClient);
        self::assertSame('my-namespace', $apiClient->getK8sNamespace());
    }

    public function testCreateApiClientThrowsWhenNoNamespaceIsAvailable(): void
    {
        $factory = new StaticKubernetesApiClientFactory(
            new RetryProxy(),
            'https://example.test',
            new StaticToken('token'),
            __DIR__ . '/../fixtures/ca.crt',
        );

        $this->expectException(InvalidArgumentException::class);

        $factory->createApiClient();
    }
}
