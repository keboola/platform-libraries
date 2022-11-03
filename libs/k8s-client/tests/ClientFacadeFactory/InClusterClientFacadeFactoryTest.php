<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory;

use Keboola\K8sClient\ClientFacadeFactory\GenericClientFacadeFactory;
use Keboola\K8sClient\ClientFacadeFactory\InClusterClientFacadeFactory;
use Keboola\K8sClient\Exception\ConfigurationException;
use Keboola\K8sClient\KubernetesApiClientFacade;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

class InClusterClientFacadeFactoryTest extends TestCase
{
    private readonly string $credentialsPath;

    protected function setUp(): void
    {
        parent::setUp();

        $this->credentialsPath = sys_get_temp_dir().'/k8s-creds-test';

        $filesystem = new Filesystem();
        $filesystem->remove($this->credentialsPath);

        $filesystem->dumpFile($this->credentialsPath.'/token', 'token');
        $filesystem->dumpFile($this->credentialsPath.'/ca.crt', 'server-cert');
        $filesystem->dumpFile($this->credentialsPath.'/namespace', 'namespace');
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        @rmdir($this->credentialsPath);
    }

    public function testCreateClusterClient(): void
    {
        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(
                'https://kubernetes.default.svc',
                'token',
                $this->credentialsPath.'/ca.crt',
                'namespace',
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

    public function testCreateClusterClientWithMissingTokenFile(): void
    {
        unlink($this->credentialsPath.'/token');

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
        unlink($this->credentialsPath.'/ca.crt');

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
        unlink($this->credentialsPath.'/namespace');

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

    public function testCreateClusterClientWithCustomNamespace(): void
    {
        $createdClient = $this->createMock(KubernetesApiClientFacade::class);

        $genericFactory = $this->createMock(GenericClientFacadeFactory::class);
        $genericFactory->expects(self::once())
            ->method('createClusterClient')
            ->with(
                'https://kubernetes.default.svc',
                'token',
                $this->credentialsPath.'/ca.crt',
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
}
