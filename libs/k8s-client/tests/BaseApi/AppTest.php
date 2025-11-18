<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\BaseApi;

use GuzzleHttp\Psr7\Response;
use Keboola\K8sClient\BaseApi\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App as TheApp;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppList;
use Keboola\K8sClient\Tests\ReflectionPropertyAccessTestCase;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\Client;
use PHPUnit\Framework\TestCase;

class AppTest extends TestCase
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
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps",
                [
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appApi = $this->getMockBuilder(App::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appApi->expects($this->once())
            ->method('parseResponse')
            ->willReturn(new AppList());

        self::setPrivatePropertyValue($appApi, 'client', $clientMock);

        $appApi->list($namespace, $queries);
    }

    public function testRead(): void
    {
        $namespace = 'default';
        $name = 'app-123';
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name",
                [
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appApi = $this->getMockBuilder(App::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'readAppsKeboolaComV1NamespacedApp')
            ->willReturn(new TheApp());

        self::setPrivatePropertyValue($appApi, 'client', $clientMock);

        $appApi->read($namespace, $name, $queries);
    }

    public function testReadStatus(): void
    {
        $namespace = 'default';
        $name = 'app-123';
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name/status",
                [
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appApi = $this->getMockBuilder(App::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'readAppsKeboolaComV1NamespacedAppStatus')
            ->willReturn(new TheApp());

        self::setPrivatePropertyValue($appApi, 'client', $clientMock);

        $appApi->readStatus($namespace, $name, $queries);
    }

    public function testCreate(): void
    {
        $namespace = 'default';
        $app = new TheApp(['metadata' => ['name' => 'app-123']]);
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'post',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps",
                [
                    'json' => $app,
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(201));

        $appApi = $this->getMockBuilder(App::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'createAppsKeboolaComV1NamespacedApp')
            ->willReturn(new TheApp());

        self::setPrivatePropertyValue($appApi, 'client', $clientMock);

        $appApi->create($namespace, $app, $queries);
    }

    public function testPatch(): void
    {
        $namespace = 'default';
        $name = 'app-123';
        $patch = new Patch(['spec' => ['state' => 'Running']]);
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'patch',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name",
                [
                    'json' => $patch,
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appApi = $this->getMockBuilder(App::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'patchAppsKeboolaComV1NamespacedApp')
            ->willReturn(new TheApp());

        self::setPrivatePropertyValue($appApi, 'client', $clientMock);

        $appApi->patch($namespace, $name, $patch, $queries);
    }

    public function testDelete(): void
    {
        $namespace = 'default';
        $name = 'app-123';
        $deleteOptions = new DeleteOptions();
        $queries = [];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'delete',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name",
                [
                    'json' => $deleteOptions,
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appApi = $this->getMockBuilder(App::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'deleteAppsKeboolaComV1NamespacedApp')
            ->willReturn(new Status());

        self::setPrivatePropertyValue($appApi, 'client', $clientMock);

        $appApi->delete($namespace, $name, $deleteOptions, $queries);
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
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps",
                [
                    'json' => $deleteOptions,
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $appApi = $this->getMockBuilder(App::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $appApi->expects($this->once())
            ->method('parseResponse')
            ->with($this->anything(), 'deleteAppsKeboolaComV1CollectionNamespacedApp')
            ->willReturn(new Status());

        self::setPrivatePropertyValue($appApi, 'client', $clientMock);

        $appApi->deleteCollection($namespace, $deleteOptions, $queries);
    }
}
