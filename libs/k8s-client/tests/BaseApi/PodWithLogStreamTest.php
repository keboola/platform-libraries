<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\PodWithLogStream;

use GuzzleHttp\Handler\CurlHandler;
use GuzzleHttp\HandlerStack;
use Keboola\K8sClient\BaseApi\PodWithLogStream;
use KubernetesRuntime\Client;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamInterface;
use ReflectionClass;

class PodWithLogStreamTest extends TestCase
{
    public function testReadLogReturnsBodyOnSuccess(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = ['foo' => 'bar'];

        $responseMock = $this->createMock(ResponseInterface::class);
        $responseMock->method('getStatusCode')->willReturn(200);
        $responseBody = $this->createMock(StreamInterface::class);
        $responseMock->method('getBody')->willReturn($responseBody);

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

                    $handlerStack = $options['handler'];
                    $reflection = new ReflectionClass($handlerStack);
                    $property = $reflection->getProperty('handler');
                    $property->setAccessible(true);
                    $handler = $property->getValue($handlerStack);
                    $this->assertInstanceOf(CurlHandler::class, $handler);

                    return true;
                }),
            )
            ->willReturn($responseMock);

        $podWithLogStream = $this->getMockBuilder(PodWithLogStream::class)
            ->setConstructorArgs([$namespace])
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $reflection = new ReflectionClass($podWithLogStream);
        $property = $reflection->getProperty('client');
        $property->setAccessible(true);
        $property->setValue($podWithLogStream, $clientMock);

        $result = $podWithLogStream->readLog($namespace, $podName, $queries);

        $this->assertEquals($responseBody, $result);
    }

    public function testReadLogCallsParseResponseOnError(): void
    {
        $namespace = 'default';
        $podName = 'test-pod';
        $queries = [];

        $responseMock = $this->createMock(ResponseInterface::class);
        $responseMock->method('getStatusCode')->willReturn(404);

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
            ->willReturn($responseMock);

        $podWithLogStream = $this->getMockBuilder(PodWithLogStream::class)
            ->setConstructorArgs([$namespace])
            ->onlyMethods(['parseResponse'])
            ->getMock();

        $podWithLogStream->expects($this->once())
            ->method('parseResponse')
            ->with($responseMock, 'readCoreV1NamespacedPodLog')
            ->willReturn('parsed response');

        $reflection = new ReflectionClass($podWithLogStream);
        $property = $reflection->getProperty('client');
        $property->setAccessible(true);
        $property->setValue($podWithLogStream, $clientMock);

        $result = $podWithLogStream->readLog($namespace, $podName, $queries);

        $this->assertEquals('parsed response', $result);
    }
}
