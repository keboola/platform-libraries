<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFactory;

use Keboola\K8sClient\ClientFactory\AutoDetectKubernetesApiClientFactory;
use Keboola\K8sClient\ClientFactory\EnvVariablesKubernetesApiClientFactory;
use Keboola\K8sClient\ClientFactory\InClusterKubernetesApiClientFactory;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClient;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class AutoDetectKubernetesApiClientFactoryTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->logsHandler]);
    }

    public static function provideCustomNamespaceValue(): iterable
    {
        yield 'default namespace' => [
            'customNamespace' => null,
        ];

        yield 'custom namespace' => [
            'customNamespace' => 'custom-namespace',
        ];
    }

    /** @dataProvider provideCustomNamespaceValue */
    public function testCreateApiClientWithEnvVars(?string $customNamespace): void
    {
        $createdClient = $this->createMock(KubernetesApiClient::class);

        $envVariablesFactory = $this->createMock(EnvVariablesKubernetesApiClientFactory::class);
        $envVariablesFactory->expects(self::once())
            ->method('isAvailable')
            ->with($customNamespace)
            ->willReturn(true);
        $envVariablesFactory->expects(self::once())
            ->method('createApiClient')
            ->with($customNamespace)
            ->willReturn($createdClient);

        $inClusterFactory = $this->createMock(InClusterKubernetesApiClientFactory::class);
        $inClusterFactory->expects(self::never())->method('isAvailable');
        $inClusterFactory->expects(self::never())->method('createApiClient');

        $factory = new AutoDetectKubernetesApiClientFactory(
            $envVariablesFactory,
            $inClusterFactory,
            $this->logger,
        );
        $result = $factory->createApiClient($customNamespace);

        self::assertSame($createdClient, $result);
        self::assertTrue(
            $this->logsHandler->hasDebugThatContains('Using ENV variables configuration for K8S client.'),
        );
    }

    /** @dataProvider provideCustomNamespaceValue */
    public function testCreateApiClientWithInClusterAuth(?string $customNamespace): void
    {
        $createdClient = $this->createMock(KubernetesApiClient::class);

        $envVariablesFactory = $this->createMock(EnvVariablesKubernetesApiClientFactory::class);
        $envVariablesFactory->expects(self::once())
            ->method('isAvailable')
            ->with($customNamespace)
            ->willReturn(false);
        $envVariablesFactory->expects(self::never())->method('createApiClient');

        $inClusterFactory = $this->createMock(InClusterKubernetesApiClientFactory::class);
        $inClusterFactory->expects(self::once())
            ->method('isAvailable')
            ->with($customNamespace)
            ->willReturn(true);
        $inClusterFactory->expects(self::once())
            ->method('createApiClient')
            ->with($customNamespace)
            ->willReturn($createdClient);

        $factory = new AutoDetectKubernetesApiClientFactory(
            $envVariablesFactory,
            $inClusterFactory,
            $this->logger,
        );
        $result = $factory->createApiClient($customNamespace);

        self::assertSame($createdClient, $result);
        self::assertTrue(
            $this->logsHandler->hasDebugThatContains('Using in-cluster configuration for K8S client.'),
        );
    }

    public function testCreateApiClientWithNoCredentialsFound(): void
    {
        $envVariablesFactory = $this->createMock(EnvVariablesKubernetesApiClientFactory::class);
        $envVariablesFactory->expects(self::once())->method('isAvailable')->willReturn(false);
        $envVariablesFactory->expects(self::never())->method('createApiClient');

        $inClusterFactory = $this->createMock(InClusterKubernetesApiClientFactory::class);
        $inClusterFactory->expects(self::once())->method('isAvailable')->willReturn(false);
        $inClusterFactory->expects(self::never())->method('createApiClient');

        $factory = new AutoDetectKubernetesApiClientFactory(
            $envVariablesFactory,
            $inClusterFactory,
            $this->logger,
        );

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('No valid K8S client configuration found.');

        $factory->createApiClient();
    }
}
