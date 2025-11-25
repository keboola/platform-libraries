<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\BaseApi;

use GuzzleHttp\Psr7\Response;
use Keboola\K8sClient\BaseApi\AppRun;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun as TheAppRun;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRunList;
use Keboola\K8sClient\Tests\ReflectionPropertyAccessTestCase;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\Client;
use PHPUnit\Framework\TestCase;

class AppRunTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testList(): void
    {
        $namespace = 'default';
        $queries = ['labelSelector' => 'app=test'];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns",
                [
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appRunApi = $this->getMockBuilder(AppRun::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appRunApi->expects($this->once())
            ->method('parseResponse')
            ->willReturn(new AppRunList());

        self::setPrivatePropertyValue($appRunApi, 'client', $clientMock);

        $appRunApi->list($namespace, $queries);
    }

    public function testRead(): void
    {
        $namespace = 'default';
        $name = 'apprun-123';
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns/$name",
                [
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appRunApi = $this->getMockBuilder(AppRun::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appRunApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'readAppsKeboolaComV1NamespacedAppRun')
            ->willReturn(new TheAppRun());

        self::setPrivatePropertyValue($appRunApi, 'client', $clientMock);

        $appRunApi->read($namespace, $name, $queries);
    }

    public function testCreate(): void
    {
        $namespace = 'default';
        $appRun = new TheAppRun(['metadata' => ['name' => 'apprun-123']]);
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'post',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns",
                [
                    'json' => $appRun->getArrayCopy(),
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(201));

        $appRunApi = $this->getMockBuilder(AppRun::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appRunApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'createAppsKeboolaComV1NamespacedAppRun')
            ->willReturn(new TheAppRun());

        self::setPrivatePropertyValue($appRunApi, 'client', $clientMock);

        $appRunApi->create($namespace, $appRun, $queries);
    }

    public function testPatch(): void
    {
        $namespace = 'default';
        $name = 'apprun-123';
        $patch = new Patch(['spec' => ['state' => 'Running']]);
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'patch',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns/$name",
                [
                    'json' => $patch->getArrayCopy(),
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appRunApi = $this->getMockBuilder(AppRun::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appRunApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'patchAppsKeboolaComV1NamespacedAppRun')
            ->willReturn(new TheAppRun());

        self::setPrivatePropertyValue($appRunApi, 'client', $clientMock);

        $appRunApi->patch($namespace, $name, $patch, $queries);
    }

    public function testDelete(): void
    {
        $namespace = 'default';
        $name = 'apprun-123';
        $deleteOptions = new DeleteOptions();
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'delete',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns/$name",
                [
                    'json' => $deleteOptions,
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appRunApi = $this->getMockBuilder(AppRun::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appRunApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'deleteAppsKeboolaComV1NamespacedAppRun')
            ->willReturn(new Status());

        self::setPrivatePropertyValue($appRunApi, 'client', $clientMock);

        $appRunApi->delete($namespace, $name, $deleteOptions, $queries);
    }

    public function testDeleteCollection(): void
    {
        $namespace = 'default';
        $deleteOptions = new DeleteOptions();
        $queries = ['labelSelector' => 'app=test'];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'delete',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns",
                [
                    'json' => $deleteOptions,
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appRunApi = $this->getMockBuilder(AppRun::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appRunApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'deleteAppsKeboolaComV1CollectionNamespacedAppRun')
            ->willReturn(new Status());

        self::setPrivatePropertyValue($appRunApi, 'client', $clientMock);

        $appRunApi->deleteCollection($namespace, $deleteOptions, $queries);
    }
}
