<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\InClusterClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\Token\InClusterToken;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Filesystem\Path;

class InClusterClientFacadeFactoryTest extends TestCase
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

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())->method('createClusterClient');

        $factory = new InClusterClientFacadeFactory($genericFactory, $this->credentialsPath);
        $result = $factory->isAvailable($customNamespace);

        self::assertSame($expectedResult, $result);
    }

    public function testCreateClusterClient(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(
                'https://kubernetes.default.svc',
                new InClusterToken($this->credentialsPath . '/token'),
                $this->credentialsPath . '/ca.crt',
                'test-namespace',
            )
            ->willReturn($createdClient)
        ;

        $factory = new InClusterClientFacadeFactory(
            $genericFactory,
            $this->credentialsPath,
        );

        $result = $factory->createClusterClient();
        self::assertSame($createdClient, $result);
    }

    public function testCreateClusterClientWithCustomNamespace(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(
                'https://kubernetes.default.svc',
                new InClusterToken($this->credentialsPath . '/token'),
                $this->credentialsPath . '/ca.crt',
                'custom-namespace',
            )
            ->willReturn($createdClient)
        ;

        $factory = new InClusterClientFacadeFactory(
            $genericFactory,
            $this->credentialsPath,
        );

        $result = $factory->createClusterClient('custom-namespace');
        self::assertSame($createdClient, $result);
    }

    public function testCreateClusterClientWithCustomNamespaceAndMissingNamespaceFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/namespace');

        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(
                'https://kubernetes.default.svc',
                new InClusterToken($this->credentialsPath . '/token'),
                $this->credentialsPath . '/ca.crt',
                'custom-namespace',
            )
            ->willReturn($createdClient)
        ;

        $factory = new InClusterClientFacadeFactory(
            $genericFactory,
            $this->credentialsPath,
        );

        $result = $factory->createClusterClient('custom-namespace');
        self::assertSame($createdClient, $result);
    }

    public function testCreateClusterClientWithMissingTokenFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/token');

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())->method('createClusterClient');

        $factory = new InClusterClientFacadeFactory(
            $genericFactory,
            $this->credentialsPath,
        );

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('In-cluster configuration file "/tmp/k8s-creds-test/token" does not exist');

        $factory->createClusterClient();
    }

    public function testCreateClusterClientWithMissingCertFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/ca.crt');

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())->method('createClusterClient');

        $factory = new InClusterClientFacadeFactory(
            $genericFactory,
            $this->credentialsPath,
        );

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('In-cluster configuration file "/tmp/k8s-creds-test/ca.crt" does not exist');

        $factory->createClusterClient();
    }

    public function testCreateClusterClientWithMissingNamespaceFile(): void
    {
        foreach (self::FILES as $file => $contents) {
            $filesystem = new Filesystem();
            $filesystem->dumpFile(Path::join($this->credentialsPath, $file), $contents);
        }
        unlink($this->credentialsPath . '/namespace');

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::never())->method('createClusterClient');

        $factory = new InClusterClientFacadeFactory(
            $genericFactory,
            $this->credentialsPath,
        );

        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('In-cluster configuration file "/tmp/k8s-creds-test/namespace" does not exist');

        $factory->createClusterClient();
    }
}
