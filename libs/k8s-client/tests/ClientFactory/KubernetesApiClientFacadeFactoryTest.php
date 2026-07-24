<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFactory;

use Keboola\K8sClient\ApiClient\ApiClientInterface;
use Keboola\K8sClient\ClientFactory\KubernetesApiClientFacadeFactory;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\KubernetesApiClientFacade;
use Keboola\K8sClient\Tests\FakeCrdModel;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;

class KubernetesApiClientFacadeFactoryTest extends TestCase
{
    public function testCreateBuildsFacadeFromApiClient(): void
    {
        $apiClient = new KubernetesApiClient(new RetryProxy(), 'my-namespace');
        $factory = new KubernetesApiClientFacadeFactory(new Logger('test'));

        $facade = $factory->create($apiClient);

        self::assertInstanceOf(KubernetesApiClientFacade::class, $facade);
    }

    public function testCreateThreadsExtraClientsThroughToFacade(): void
    {
        $apiClient = new KubernetesApiClient(new RetryProxy(), 'my-namespace');
        $factory = new KubernetesApiClientFacadeFactory(new Logger('test'));

        $extraClient = $this->createMock(ApiClientInterface::class);

        $facade = $factory->create($apiClient, [FakeCrdModel::class => $extraClient]);

        self::assertSame($extraClient, $facade->client(FakeCrdModel::class));
    }

    public function testCreateThreadsExtraClientMergePatchRouting(): void
    {
        $apiClient = new KubernetesApiClient(new RetryProxy(), 'my-namespace');
        $factory = new KubernetesApiClientFacadeFactory(new Logger('test'));

        $model = new FakeCrdModel(['metadata' => ['name' => 'thing-1'], 'spec' => ['size' => 2]]);

        $extraClient = $this->createMock(ApiClientInterface::class);
        $extraClient->expects(self::once())
            ->method('patch')
            ->willReturnCallback(function (string $name, Patch $patch) use ($model) {
                self::assertSame('thing-1', $name);
                $data = $patch->getArrayCopy();
                self::assertSame('merge-patch', $data['patchOperation']);
                self::assertSame(2, $data['spec']['size']);
                return $model;
            });

        $facade = $factory->create($apiClient, [FakeCrdModel::class => $extraClient]);

        self::assertSame($model, $facade->mergePatch($model));
    }
}
