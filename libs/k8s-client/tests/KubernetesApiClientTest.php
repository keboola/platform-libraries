<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests;

use Generator;
use Keboola\K8sClient\Exception\KubernetesResponseException;
use Keboola\K8sClient\Exception\ResourceAlreadyExistsException;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Event as EventsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Event;
use Kubernetes\Model\Io\K8s\Api\Core\V1\EventList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use PHPUnit\Framework\TestCase;
use Retry\RetryProxy;

class KubernetesApiClientTest extends TestCase
{
    private const TEST_NAMESPACE = 'test-namespace';

    public function testGetK8sNamespace(): void
    {
        $client = new KubernetesApiClient(
            $this->createMock(RetryProxy::class),
            self::TEST_NAMESPACE,
        );

        self::assertSame(self::TEST_NAMESPACE, $client->getK8sNamespace());
    }

    public function testClusterRequest(): void
    {
        $client = new KubernetesApiClient(
            $this->createRetryProxyMock(),
            self::TEST_NAMESPACE,
        );

        $event = new Event(['name' => 'test-event']);

        $eventsApiMock = $this->createMock(EventsApi::class);
        $eventsApiMock->expects(self::once())
            ->method('read')
            ->with(self::TEST_NAMESPACE, 'event-name')
            ->willReturn($event)
        ;

        $result = $client->clusterRequest(
            $eventsApiMock,
            'read',
            Event::class,
            self::TEST_NAMESPACE,
            'event-name',
        );

        self::assertSame($event, $result);
    }

    public function clusterRequestFailsWithStatusInResultProvider(): Generator
    {
        $status = new Status();
        $status->code = 500;
        $status->message = 'Some error from K8S';

        yield 'application error' => [
            'method' => 'list',
            'methodArguments' => [
                self::TEST_NAMESPACE,
                ['test'],
            ],
            'status' => $status,
            'expectedErrorMessage' => 'K8S request has failed: Some error from K8S',
            'expectetErrorClassName' => KubernetesResponseException::class,
        ];

        $status = new Status();
        $status->code = 404;
        $status->status = 'Failure';
        $status->message = 'Some error from K8S';

        yield 'not found failure' => [
            'method' => 'list',
            'methodArguments' => [
                self::TEST_NAMESPACE,
                ['test'],
            ],
            'status' => $status,
            'expectedErrorMessage' => 'Resource not found: Some error from K8S',
            'expectetErrorClassName' => ResourceNotFoundException::class,
        ];

        $status = new Status();
        $status->status = 'Failure';
        $status->reason = 'AlreadyExists';
        $status->message = 'Some error from K8S';

        yield 'already exists' => [
            'method' => 'create',
            'methodArguments' => [
                self::TEST_NAMESPACE,
                $this->createMock(Event::class),
            ],
            'status' => $status,
            'expectedErrorMessage' => 'Resource already exists: Some error from K8S',
            'expectetErrorClassName' => ResourceAlreadyExistsException::class,
        ];

        $status = new Status();
        $status->status = 'Failure';
        $status->reason = 'Random';
        $status->message = 'Some error from K8S';

        yield 'random error' => [
            'method' => 'create',
            'methodArguments' => [
                self::TEST_NAMESPACE,
                $this->createMock(Event::class),
            ],
            'status' => $status,
            'expectedErrorMessage' => 'K8S request has failed: Some error from K8S',
            'expectetErrorClassName' => KubernetesResponseException::class,
        ];
    }

    /**
     * @dataProvider clusterRequestFailsWithStatusInResultProvider
     */
    public function testClusterRequestFailsWithStatusInResult(
        string $method,
        array $methodArguments,
        Status $status,
        string $expectedErrorMessage,
        string $expectetErrorClassName,
    ): void {
        $client = new KubernetesApiClient(
            $this->createRetryProxyMock(),
            self::TEST_NAMESPACE,
        );

        $eventsApiMock = $this->createMock(EventsApi::class);
        $eventsApiMock->expects(self::once())
            ->method($method)
            ->with(...$methodArguments)
            ->willReturn($status)
        ;

        try {
            $client->clusterRequest(
                $eventsApiMock,
                $method,
                EventList::class,
                ...$methodArguments,
            );

            $this->fail('Cluster request should throw KubernetesResponseException');
        } catch (KubernetesResponseException $e) {
            self::assertSame($expectetErrorClassName, get_class($e));
            self::assertSame($status, $e->getStatus());
            self::assertSame($expectedErrorMessage, $e->getMessage());
        }
    }

    public function testClusterRequestFailsOnUnexpectedResult(): void
    {
        $client = new KubernetesApiClient(
            $this->createRetryProxyMock(),
            self::TEST_NAMESPACE,
        );

        $resultMock = $this->createMock(EventList::class);

        $eventsApiMock = $this->createMock(EventsApi::class);
        $eventsApiMock->expects(self::once())
            ->method('read')
            ->with(self::TEST_NAMESPACE, 'event-name')
            ->willReturn($resultMock)
        ;

        try {
            $client->clusterRequest(
                $eventsApiMock,
                'read',
                Event::class,
                self::TEST_NAMESPACE,
                'event-name',
            );

            $this->fail('Cluster request should throw KubernetesResponseException');
        } catch (KubernetesResponseException $e) {
            self::assertNull($e->getStatus());
            self::assertSame(
                sprintf(
                    'Expected response class %s for request %s::read, found %s',
                    Event::class,
                    get_class($eventsApiMock),
                    get_debug_type($resultMock),
                ),
                $e->getMessage(),
            );
        }
    }

    public function clusterRequestFailsOnRawResultProvider(): Generator
    {
        yield 'plain text body' => [
            'result' => '502 Bad Gateway',
            'expectedFoundPart' => 'found string: "502 Bad Gateway"',
        ];

        yield 'json body without kind' => [
            'result' => ['detail' => 'no kind here'],
            'expectedFoundPart' => 'found array: "{"detail":"no kind here"}"',
        ];

        yield 'long body is truncated' => [
            'result' => str_repeat('a', 600),
            'expectedFoundPart' => sprintf('found string: "%s..."', str_repeat('a', 512)),
        ];
    }

    /**
     * @dataProvider clusterRequestFailsOnRawResultProvider
     */
    public function testClusterRequestFailsOnRawResult(
        mixed $result,
        string $expectedFoundPart,
    ): void {
        $client = new KubernetesApiClient(
            $this->createRetryProxyMock(),
            self::TEST_NAMESPACE,
        );

        $eventsApiMock = $this->createMock(EventsApi::class);
        $eventsApiMock->expects(self::once())
            ->method('read')
            ->with(self::TEST_NAMESPACE, 'event-name')
            ->willReturn($result)
        ;

        try {
            $client->clusterRequest(
                $eventsApiMock,
                'read',
                Event::class,
                self::TEST_NAMESPACE,
                'event-name',
            );

            $this->fail('Cluster request should throw KubernetesResponseException');
        } catch (KubernetesResponseException $e) {
            self::assertNull($e->getStatus());
            self::assertSame(
                sprintf(
                    'Expected response class %s for request %s::read, %s',
                    Event::class,
                    get_class($eventsApiMock),
                    $expectedFoundPart,
                ),
                $e->getMessage(),
            );
        }
    }

    private function createRetryProxyMock(): RetryProxy
    {
        $retryProxyMock = $this->createMock(RetryProxy::class);
        $retryProxyMock
            ->method('call')
            ->willReturnCallback(function (callable $callback) {
                return $callback();
            })
        ;
        return $retryProxyMock;
    }

    public function testRequestCallsClusterRequest(): void
    {
        $eventsApiMock = $this->createMock(EventsApi::class);

        $clientMock = $this->getMockBuilder(KubernetesApiClient::class)
            ->setConstructorArgs([
                $this->createMock(RetryProxy::class),
                self::TEST_NAMESPACE,
            ])
            ->onlyMethods(['clusterRequest'])
            ->getMock()
        ;

        $clientMock->expects(self::once())
            ->method('clusterRequest')
            ->with(
                $eventsApiMock,
                'read',
                Event::class,
                self::TEST_NAMESPACE,
                'dummy-name',
                ['test'],
            )
        ;

        $clientMock->request(
            $eventsApiMock,
            'read',
            Event::class,
            'dummy-name',
            ['test'],
        );
    }
}
