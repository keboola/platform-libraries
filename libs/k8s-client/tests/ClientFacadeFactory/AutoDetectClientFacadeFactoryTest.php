<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\AutoDetectClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\EnvVariablesClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\InClusterClientFacadeFactory;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class AutoDetectClientFacadeFactoryTest extends TestCase
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
    public function testCreateClientWithEnvVars(?string $customNamespace): void
    {
        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $envVariablesFactory = $this->createMock(EnvVariablesClientFacadeFactory::class);
        $envVariablesFactory->expects(self::once())->method('isAvailable')->willReturn(true);
        $envVariablesFactory->expects(self::once())
            ->method('createClusterClient')
            ->with($customNamespace)
            ->willReturn($createdClient);

        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::never())->method('isAvailable');
        $inClusterFactory->expects(self::never())->method('createClusterClient');

        $factory = new AutoDetectClientFacadeFactory(
            $envVariablesFactory,
            $inClusterFactory,
            $this->logger,
        );
        $result = $factory->createClusterClient($customNamespace);

        self::assertSame($createdClient, $result);
        self::assertTrue($this->logsHandler->hasDebugThatContains('Using ENV variables configuration for K8S client.'));
    }

    /** @dataProvider provideCustomNamespaceValue */
    public function testCreateClientWithInClusterAuth(?string $customNamespace): void
    {
        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $envVariablesFactory = $this->createMock(EnvVariablesClientFacadeFactory::class);
        $envVariablesFactory->expects(self::once())->method('isAvailable')->willReturn(false);
        $envVariablesFactory->expects(self::never())->method('createClusterClient');

        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::once())->method('isAvailable')->willReturn(true);
        $inClusterFactory->expects(self::once())
            ->method('createClusterClient')
            ->with($customNamespace)
            ->willReturn($createdClient);

        $factory = new AutoDetectClientFacadeFactory(
            $envVariablesFactory,
            $inClusterFactory,
            $this->logger,
        );
        $result = $factory->createClusterClient($customNamespace);

        self::assertSame($createdClient, $result);
        self::assertTrue($this->logsHandler->hasDebugThatContains('Using in-cluster configuration for K8S client.'));
    }

    public function testCreateClusterClientWithNoCredentialsFound(): void
    {
        $envVariablesFactory = $this->createMock(EnvVariablesClientFacadeFactory::class);
        $envVariablesFactory->expects(self::once())->method('isAvailable')->willReturn(false);
        $envVariablesFactory->expects(self::never())->method('createClusterClient');

        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::once())->method('isAvailable')->willReturn(false);
        $inClusterFactory->expects(self::never())->method('createClusterClient');

        $factory = new AutoDetectClientFacadeFactory(
            $envVariablesFactory,
            $inClusterFactory,
            $this->logger,
        );

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('No valid K8S client configuration found.');

        $factory->createClusterClient();
    }
}
