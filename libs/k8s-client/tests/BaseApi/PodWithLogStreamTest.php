<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\BaseApi;

use Generator;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Stream;
use Keboola\K8sClient\BaseApi\Data\WatchEvent;
use Keboola\K8sClient\BaseApi\Data\WatchEventType;
use Keboola\K8sClient\BaseApi\PodWithLogStream;
use Keboola\K8sClient\Tests\ReflectionPropertyAccessTestCase;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use KubernetesRuntime\Client;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\StreamInterface;

class PodWithLogStreamTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    protected function setUp(): void
    {
        parent::setUp();

        // Create a mock client and set it as the static instance
        $clientMock = $this->createMock(Client::class);
        self::setPrivateStaticPropertyValue(Client::class, 'instance', $clientMock);
    }

    public function testReadLogReturnsBodyOnSuccess(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = ['foo' => 'bar'];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                sprintf('/api/v1/namespaces/%s/pods/%s/log', $namespace, $podName),
                [
                    'query' => $queries,
                ],
            )
            ->willReturn(new Response(200));

        $podWithLogStream = $this->getMockBuilder(PodWithLogStream::class)
            ->setConstructorArgs([$namespace])
            ->onlyMethods(['parseResponse'])
            ->getMock();

        self::setPrivatePropertyValue($podWithLogStream, 'client', $clientMock);

        $result = $podWithLogStream->readLog($namespace, $podName, $queries);

        $this->assertInstanceOf(StreamInterface::class, $result);
    }

    public function testReadLogCallsParseResponseOnError(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = [];

        $response = new Response(404);

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                sprintf('/api/v1/namespaces/%s/pods/%s/log', $namespace, $podName),
                [
                    'query' => $queries,
                ],
            )
            ->willReturn($response);

        $podWithLogStream = $this->getMockBuilder(PodWithLogStream::class)
            ->setConstructorArgs([$namespace])
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $podWithLogStream->expects($this->once())
            ->method('parseResponse')
            ->with($response, 'readCoreV1NamespacedPodLog')
            ->willReturn('parsed response');

        self::setPrivatePropertyValue($podWithLogStream, 'client', $clientMock);

        $result = $podWithLogStream->readLog($namespace, $podName, $queries);

        $this->assertEquals('parsed response', $result);
    }

    public function testWatchReturnsWatchEvents(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = ['foo' => 'bar'];

        // Create a stream with JSON events
        $stream = fopen('php://memory', 'r+');
        self::assertIsResource($stream);
        ;

        fwrite($stream, <<<EOF
          {"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod"}}}
          {"type":"MODIFIED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod"}}}
        EOF);
        rewind($stream);

        $response = new Response(200, ['Content-Type' => 'application/json'], new Stream($stream));

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                sprintf('/api/v1/watch/namespaces/%s/pods/%s', $namespace, $podName),
                [
                    'stream' => true,
                    'query' => $queries,
                ],
            )
            ->willReturn($response)
        ;

        $podWithLogStream = new PodWithLogStream($namespace);
        self::setPrivatePropertyValue($podWithLogStream, 'client', $clientMock);

        $events = [...$podWithLogStream->watch($namespace, $podName, $queries)];

        $this->assertCount(2, $events);
        $this->assertInstanceOf(WatchEvent::class, $events[0]);
        $this->assertInstanceOf(WatchEvent::class, $events[1]);
        $this->assertEquals(WatchEventType::Added, $events[0]->type);
        $this->assertEquals(WatchEventType::Modified, $events[1]->type);
        $this->assertInstanceOf(Pod::class, $events[0]->object);
        $this->assertInstanceOf(Pod::class, $events[1]->object);
    }

    public function testWatchWithReadWaitTimeout(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = ['read_wait_timeout' => 5, 'foo' => 'bar'];

        // Create a stream with JSON events
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        self::assertIsArray($sockets);
        $readStream = $sockets[0];
        $writeStream = $sockets[1];

        $response = new Response(200, ['Content-Type' => 'application/json'], new Stream($readStream));

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                sprintf('/api/v1/watch/namespaces/%s/pods/%s', $namespace, $podName),
                [
                    'stream' => true,
                    'query' => ['foo' => 'bar'], // read_wait_timeout should be removed
                ],
            )
            ->willReturn($response)
        ;

        $podWithLogStream = new PodWithLogStream($namespace);
        self::setPrivatePropertyValue($podWithLogStream, 'client', $clientMock);

        $events = ($podWithLogStream->watch($namespace, $podName, $queries));
        self::assertInstanceOf(Generator::class, $events);

        // the first event should be produced normally
        fwrite($writeStream, <<<EOF
          {"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod"}}}
        EOF);

        $event = $events->current();
        $this->assertInstanceOf(WatchEvent::class, $event);
        $this->assertEquals(WatchEventType::Added, $event->type);
        $this->assertInstanceOf(Pod::class, $event->object);

        // no second event is present, NULL should be produced after timeout
        $startTimestamp = microtime(true);
        $events->next();
        $endTimestamp = microtime(true);

        self::assertEqualsWithDelta(5, $endTimestamp - $startTimestamp, 0.1);

        $event = $events->current();
        self::assertNull($event);

        // a new event is produced again after timeout
        fwrite($writeStream, <<<EOF
          {"type":"MODIFIED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod"}}}
        EOF);
        $events->next();

        $event = $events->current();
        $this->assertInstanceOf(WatchEvent::class, $event);
        $this->assertEquals(WatchEventType::Modified, $event->type);
        $this->assertInstanceOf(Pod::class, $event->object);
    }

    public function testWatchCallsParseResponseOnError(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = [];

        $response = new Response(404);

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                'get',
                sprintf('/api/v1/watch/namespaces/%s/pods/%s', $namespace, $podName),
                [
                    'stream' => true,
                    'query' => $queries,
                ],
            )
            ->willReturn($response);

        // Set the mock as the static instance
        self::setPrivateStaticPropertyValue(Client::class, 'instance', $clientMock);

        // Create a test-specific subclass that overrides parseResponse
        $testPodWithLogStream = new class($namespace) extends PodWithLogStream {
            public function parseResponse($response, $operation)
            {
                return 'parsed response';
            }
        };

        self::setPrivatePropertyValue($testPodWithLogStream, 'client', $clientMock);

        $result = $testPodWithLogStream->watch($namespace, $podName, $queries);

        $this->assertEquals('parsed response', $result);
    }
}
