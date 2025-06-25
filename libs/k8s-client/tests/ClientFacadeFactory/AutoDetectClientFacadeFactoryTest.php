<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\AutoDetectClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\InClusterClientFacadeFactory;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use PHPUnit\Framework\TestCase;

/** @runTestsInSeparateProcesses */
class AutoDetectClientFacadeFactoryTest extends TestCase
{
    private const ENV_VARS = [
        'K8S_HOST' => 'https://k8s.example.com',
        'K8S_TOKEN' => 'test-token',
        'K8S_CA_CERT_PATH' => '/path/to/ca.crt',
        'K8S_NAMESPACE' => 'test-namespace',
    ];

    public function testCreateClientWithEnvVars(): void
    {
        foreach (self::ENV_VARS as $key => $value) {
            putenv(sprintf('%s=%s', $key, $value));
        }

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(
                self::ENV_VARS['K8S_HOST'],
                self::ENV_VARS['K8S_TOKEN'],
                self::ENV_VARS['K8S_CA_CERT_PATH'],
                self::ENV_VARS['K8S_NAMESPACE'],
            )
            ->willReturn($createdClient);

        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::never())
            ->method('createClusterClient');

        $factory = new AutoDetectClientFacadeFactory($genericFactory, $inClusterFactory);
        $result = $factory->createClusterClient();
        self::assertSame($createdClient, $result);
    }

    public function testCreateClientWithEnvVarsAndCustomNamespace(): void
    {
        foreach (self::ENV_VARS as $key => $value) {
            putenv("$key=$value");
        }

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        // Mock the GenericClientFacadeFactory
        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(
                self::ENV_VARS['K8S_HOST'],
                self::ENV_VARS['K8S_TOKEN'],
                self::ENV_VARS['K8S_CA_CERT_PATH'],
                'custom-namespace',
            )
            ->willReturn($createdClient);

        // Mock the InClusterClientFacadeFactory
        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::never())
            ->method('createClusterClient');

        // Create the factory with mocked dependencies
        $factory = new AutoDetectClientFacadeFactory($genericFactory, $inClusterFactory);

        $result = $factory->createClusterClient('custom-namespace');
        self::assertSame($createdClient, $result);
    }

    public function testCreateClientWithInClusterAuth(): void
    {
        foreach (array_keys(self::ENV_VARS) as $key) {
            putenv($key);
        }

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())
            ->method('createClusterClient');

        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(null)
            ->willReturn($createdClient);

        $factory = new AutoDetectClientFacadeFactory($genericFactory, $inClusterFactory);
        $result = $factory->createClusterClient();
        self::assertSame($createdClient, $result);
    }

    public function testCreateClientWithInClusterAuthAndCustomNamespace(): void
    {
        foreach (array_keys(self::ENV_VARS) as $key) {
            putenv($key);
        }

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        // Mock the GenericClientFacadeFactory
        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())
            ->method('createClusterClient');

        // Mock the InClusterClientFacadeFactory
        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::once())
            ->method('createClusterClient')
            ->with('custom-namespace')
            ->willReturn($createdClient);

        // Create the factory with mocked dependencies
        $factory = new AutoDetectClientFacadeFactory($genericFactory, $inClusterFactory);

        $result = $factory->createClusterClient('custom-namespace');
        self::assertSame($createdClient, $result);
    }

    public static function provideEnvToUnset(): iterable
    {
        yield 'no K8S_HOST' => [
            'env' => 'K8S_HOST',
        ];

        yield 'no K8S_TOKEN' => [
            'env' => 'K8S_TOKEN',
        ];

        yield 'no K8S_CA_CERT_PATH' => [
            'env' => 'K8S_CA_CERT_PATH',
        ];
    }

    /** @dataProvider provideEnvToUnset */
    public function testInClusterClientIsUsedWhenSomeEnvIsMissing(string $env): void
    {
        // set all ENVs but one
        foreach (self::ENV_VARS as $key => $value) {
            putenv("$key=$value");
        }
        putenv($env); // clear single ENV

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())
            ->method('createClusterClient');

        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(null)
            ->willReturn($createdClient);

        $factory = new AutoDetectClientFacadeFactory($genericFactory, $inClusterFactory);

        $result = $factory->createClusterClient();
        self::assertSame($createdClient, $result);
    }

    public function testCreateClusterClientWithNoCredentialsFound(): void
    {
        foreach (array_keys(self::ENV_VARS) as $key) {
            putenv($key);
        }

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())
            ->method('createClusterClient');

        $inClusterFactory = $this->createMock(InClusterClientFacadeFactory::class);
        $inClusterFactory->expects(self::once())
            ->method('createClusterClient')
            ->willThrowException(new ConfigurationException('In-cluster configuration failed'));

        $factory = new AutoDetectClientFacadeFactory($genericFactory, $inClusterFactory);

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('No valid K8S client configuration found.');

        $factory->createClusterClient();
    }
}
