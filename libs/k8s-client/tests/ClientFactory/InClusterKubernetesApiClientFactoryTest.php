<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFactory;

use Keboola\K8sClient\ClientFactory\InClusterKubernetesApiClientFactory;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClient;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Filesystem\Path;

class InClusterKubernetesApiClientFactoryTest extends TestCase
{
    private const FILES = [
        'token' => 'test-token',
        'ca.crt' => 'test-cert',
        'namespace' => 'test-namespace',
    ];

    private readonly string $credentialsPath;

    protected function setUp(): void
    {
        parent::setUp();

        $this->credentialsPath = sys_get_temp_dir().'/k8s-creds-test';

        $filesystem = new Filesystem();
        $filesystem->remove($this->credentialsPath);
    }

    protected function tearDown(): void
    {
        $filesystem = new Filesystem();
        $filesystem->remove($this->credentialsPath);

        parent::tearDown();
    }

    public static function provideIsAvailableTestData(): iterable
    {
        yield 'no files' => [
            'existingFiles' => [],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'missing token file' => [
            'existingFiles' => [
                'ca.crt' => 'test-cert',
                'namespace' => 'test-namespace',
            ],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'missing cert file' => [
            'existingFiles' => [
                'token' => 'test-token',
                'namespace' => 'test-namespace',
            ],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'missing namespace file' => [
            'existingFiles' => [
                'token' => 'test-token',
                'ca.crt' => 'test-cert',
            ],
            'customNamespace' => null,
            'expectedResult' => false,
        ];

        yield 'missing namespace file with custom namespace' => [
            'existingFiles' => [
                'token' => 'test-token',
                'ca.crt' => 'test-cert',
            ],
            'customNamespace' => 'custom-namespace',
            'expectedResult' => true,
        ];

        yield 'all files' => [
            'existingFiles' => self::FILES,
            'customNamespace' => null,
            'expectedResult' => true,
        ];
    }

    /** @dataProvider provideIsAvailableTestData */
    public function testIsAvailable(array $existingFiles, ?string $customNamespace, bool $expectedResult): void
    {
        foreach ($existingFiles as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }

        $factory = new InClusterKubernetesApiClientFactory(new RetryProxy(), $this->credentialsPath);
        $result = $factory->isAvailable($customNamespace);

        self::assertSame($expectedResult, $result);
    }

    public function testCreateApiClient(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }

        $factory = new InClusterKubernetesApiClientFactory(new RetryProxy(), $this->credentialsPath);

        $result = $factory->createApiClient();

        self::assertInstanceOf(KubernetesApiClient::class, $result);
        self::assertSame('test-namespace', $result->getK8sNamespace());
    }

    public function testCreateApiClientWithCustomNamespace(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }

        $factory = new InClusterKubernetesApiClientFactory(new RetryProxy(), $this->credentialsPath);

        $result = $factory->createApiClient('custom-namespace');

        self::assertSame('custom-namespace', $result->getK8sNamespace());
    }

    public function testCreateApiClientWithCustomNamespaceAndMissingNamespaceFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/namespace');

        $factory = new InClusterKubernetesApiClientFactory(new RetryProxy(), $this->credentialsPath);

        $result = $factory->createApiClient('custom-namespace');
        self::assertSame('custom-namespace', $result->getK8sNamespace());
    }

    public function testCreateApiClientWithMissingTokenFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/token');

        $factory = new InClusterKubernetesApiClientFactory(new RetryProxy(), $this->credentialsPath);

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage(sprintf(
            'In-cluster configuration file "%s/token" does not exist',
            $this->credentialsPath,
        ));

        $factory->createApiClient();
    }

    public function testCreateApiClientWithMissingCertFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/ca.crt');

        $factory = new InClusterKubernetesApiClientFactory(new RetryProxy(), $this->credentialsPath);

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage(sprintf(
            'In-cluster configuration file "%s/ca.crt" does not exist',
            $this->credentialsPath,
        ));

        $factory->createApiClient();
    }

    public function testCreateApiClientWithMissingNamespaceFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/namespace');

        $factory = new InClusterKubernetesApiClientFactory(new RetryProxy(), $this->credentialsPath);

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage(sprintf(
            'In-cluster configuration file "%s/namespace" does not exist',
            $this->credentialsPath,
        ));

        $factory->createApiClient();
    }
}
