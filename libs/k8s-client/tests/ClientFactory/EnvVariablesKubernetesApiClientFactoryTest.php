<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFactory;

use Keboola\K8sClient\ClientFactory\EnvVariablesKubernetesApiClientFactory;
use Keboola\K8sClient\KubernetesApiClient;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;
use RuntimeException;

/** @runTestsInSeparateProcesses */
class EnvVariablesKubernetesApiClientFactoryTest extends TestCase
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

        $factory = new EnvVariablesKubernetesApiClientFactory(new RetryProxy());
        $result = $factory->isAvailable($customNamespace);

        self::assertSame($expectedResult, $result);
    }

    public function testCreateApiClient(): void
    {
        $envs = self::ENV_VARS;
        $envs['K8S_CA_CERT_PATH'] = __DIR__ . '/../fixtures/ca.crt';

        foreach ($envs as $key => $value) {
            putenv(sprintf('%s=%s', $key, $value));
        }

        $factory = new EnvVariablesKubernetesApiClientFactory(new RetryProxy());
        $client = $factory->createApiClient();

        self::assertInstanceOf(KubernetesApiClient::class, $client);
        self::assertSame(self::ENV_VARS['K8S_NAMESPACE'], $client->getK8sNamespace());
    }

    public function testCreateApiClientWithCustomNamespace(): void
    {
        $envs = self::ENV_VARS;
        $envs['K8S_CA_CERT_PATH'] = __DIR__ . '/../fixtures/ca.crt';

        foreach ($envs as $key => $value) {
            putenv(sprintf('%s=%s', $key, $value));
        }

        $factory = new EnvVariablesKubernetesApiClientFactory(new RetryProxy());
        $client = $factory->createApiClient('custom-namespace');

        self::assertSame('custom-namespace', $client->getK8sNamespace());
    }

    public function testCreateApiClientWithInvalidConfig(): void
    {
        foreach (array_keys(self::ENV_VARS) as $key) {
            putenv($key);
        }

        $factory = new EnvVariablesKubernetesApiClientFactory(new RetryProxy());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Configuration is not complete. Use isAvailable() to check if the factory can be used.',
        );

        $factory->createApiClient();
    }
}
