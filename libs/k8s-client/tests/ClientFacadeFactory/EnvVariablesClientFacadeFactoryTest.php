<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\EnvVariablesClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\Token\StaticToken;
use Keboola\K8sClient\KubernetesApiClientFacade;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/** @runTestsInSeparateProcesses */
class EnvVariablesClientFacadeFactoryTest extends TestCase
{
    private const ENV_VARS = [
        'K8S_HOST' => 'https://k8s.example.com',
        'K8S_TOKEN' => 'test-token',
        'K8S_CA_CERT_PATH' => '/path/to/ca.crt',
        'K8S_NAMESPACE' => 'test-namespace',
    ];

    public static function provideIsAvailableTestData(): iterable
    {
        yield 'no ENV' => [
            'envs' => [],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'no K8S_HOST' => [
            'envs' => [
                'K8S_TOKEN' => 'test-token',
                'K8S_CA_CERT_PATH' => '/path/to/ca.crt',
                'K8S_NAMESPACE' => 'test-namespace',
            ],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'no K8S_TOKEN' => [
            'envs' => [
                'K8S_HOST' => 'https://k8s.example.com',
                'K8S_CA_CERT_PATH' => '/path/to/ca.crt',
                'K8S_NAMESPACE' => 'test-namespace',
            ],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'no K8S_CA_CERT_PATH' => [
            'envs' => [
                'K8S_HOST' => 'https://k8s.example.com',
                'K8S_TOKEN' => 'test-token',
                'K8S_NAMESPACE' => 'test-namespace',
            ],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'no K8S_NAMESPACE' => [
            'envs' => [
                'K8S_HOST' => 'https://k8s.example.com',
                'K8S_TOKEN' => 'test-token',
                'K8S_CA_CERT_PATH' => '/path/to/ca.crt',
            ],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'no K8S_NAMESPACE with custom namespace' => [
            'envs' => [
                'K8S_HOST' => 'https://k8s.example.com',
                'K8S_TOKEN' => 'test-token',
                'K8S_CA_CERT_PATH' => '/path/to/ca.crt',
            ],
            'customNamespace' => 'custom-namespace',
            'expectedResult' => true,
        ];

        yield 'all ENV' => [
            'envs' => self::ENV_VARS,
            'customNamespace' => null,
            'expectedResult' => true,
        ];
    }

    /** @dataProvider provideIsAvailableTestData */
    public function testIsAvailable(array $envs, ?string $customNamespace, bool $expectedResult): void
    {
        foreach (self::ENV_VARS as $key => $value) {
            putenv($key);
        }
        foreach ($envs as $key => $value) {
            putenv(sprintf('%s=%s', $key, $value));
        }

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())->method('createClusterClient');

        $factory = new EnvVariablesClientFacadeFactory($genericFactory);
        $result = $factory->isAvailable($customNamespace);

        self::assertSame($expectedResult, $result);
    }

    public function testCreateClusterClient(): void
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
                new StaticToken(self::ENV_VARS['K8S_TOKEN']),
                self::ENV_VARS['K8S_CA_CERT_PATH'],
                self::ENV_VARS['K8S_NAMESPACE'],
            )
            ->willReturn($createdClient)
        ;

        $factory = new EnvVariablesClientFacadeFactory($genericFactory);
        $client = $factory->createClusterClient();

        self::assertSame($createdClient, $client);
    }

    public function testCreateClusterClientWithCustomNamespace(): void
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
                new StaticToken(self::ENV_VARS['K8S_TOKEN']),
                self::ENV_VARS['K8S_CA_CERT_PATH'],
                'custom-namespace',
            )
            ->willReturn($createdClient)
        ;

        $factory = new EnvVariablesClientFacadeFactory($genericFactory);
        $client = $factory->createClusterClient('custom-namespace');

        self::assertSame($createdClient, $client);
    }

    public function testCreateClientWithInvalidConfig(): void
    {
        foreach (array_keys(self::ENV_VARS) as $key) {
            putenv($key);
        }

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())->method('createClusterClient');

        $factory = new EnvVariablesClientFacadeFactory($genericFactory);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Configuration is not complete. Use isAvailable() to check if the factory can be used.',
        );

        $factory->createClusterClient();
    }
}
