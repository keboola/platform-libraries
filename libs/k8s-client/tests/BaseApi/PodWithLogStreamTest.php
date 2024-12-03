<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\PodWithLogStream;

use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use Keboola\K8sClient\BaseApi\PodWithLogStream;
use Keboola\K8sClient\Tests\ReflectionPropertyAccessTestCase;
use KubernetesRuntime\Client;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\StreamInterface;

class PodWithLogStreamTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testReadLogReturnsBodyOnSuccess(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = ['foo' => 'bar'];

        $clientMock = $this->createMock(Client::class);
        $clientMock->expects($this->once())
            ->method('request')
            ->with(
                $this->equalTo('get'),
                $this->equalTo(sprintf('/api/v1/namespaces/%s/pods/%s/log', $namespace, $podName)),
                $this->callback(function ($options) use ($queries) {
                    $this->assertArrayHasKey('query', $options);
                    $this->assertEquals($queries, $options['query']);

                    // Check that 'handler' is set and is an instance of HandlerStack
                    $this->assertArrayHasKey('handler', $options);
                    $this->assertInstanceOf(HandlerStack::class, $options['handler']);

                    return true;
                }),
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
                $this->equalTo('get'),
                $this->equalTo(sprintf('/api/v1/namespaces/%s/pods/%s/log', $namespace, $podName)),
                $this->callback(function ($options) use ($queries) {
                    $this->assertArrayHasKey('query', $options);
                    $this->assertEquals($queries, $options['query']);

                    // Check that 'handler' is set and is an instance of HandlerStack
                    $this->assertArrayHasKey('handler', $options);
                    $this->assertInstanceOf(HandlerStack::class, $options['handler']);

                    return true;
                }),
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
}
