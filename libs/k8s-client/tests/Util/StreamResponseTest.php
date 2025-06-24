<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\Util;

use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Psr7\Stream;
use Keboola\K8sClient\Util\StreamResponse;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class StreamResponseTest extends TestCase
{
    public static function provideJsonToChunk(): iterable
    {
        yield 'simple json objects' => [
            'jsonContent' => '{"key1":"value1"}{"key2":"value2"}',
            'expectedChunks' => ['{"key1":"value1"}', '{"key2":"value2"}'],
        ];

        yield 'nested json objects' => [
            'jsonContent' => '{"key1":{"nested":"value"}}{"key2":"value2"}',
            'expectedChunks' => ['{"key1":{"nested":"value"}}', '{"key2":"value2"}'],
        ];

        yield 'json objects with whitespace' => [
            'jsonContent' => '{"key1":"value1"} {"key2":"value2"}',
            'expectedChunks' => ['{"key1":"value1"}', '{"key2":"value2"}'],
        ];

        yield 'complex json objects' => [
            'jsonContent' => '{"key1":{"nested":{"deep":"value"}}}{"key2":[1,2,3]}',
            'expectedChunks' => ['{"key1":{"nested":{"deep":"value"}}}', '{"key2":[1,2,3]}'],
        ];

        yield 'empty json objects' => [
            'jsonContent' => '{}{}',
            'expectedChunks' => ['{}', '{}'],
        ];
    }

    /** @dataProvider provideJsonToChunk */
    public function testChunkStreamResponse(string $jsonContent, array $expectedChunks): void
    {
        $stream = fopen('php://memory', 'r+');
        self::assertIsResource($stream);
        fwrite($stream, $jsonContent);
        rewind($stream);

        $response = new Response(
            200,
            ['Content-Type' => 'application/json'],
            new Stream($stream),
        );

        $counter = 0;
        foreach (StreamResponse::chunkStreamResponse($response, 30) as $index => $chunk) {
            $counter += 1;
            self::assertEquals($expectedChunks[$index], $chunk);
        }

        self::assertSame(count($expectedChunks), $counter);
    }

    public function testChunkStreamResponseWithInvalidContentType(): void
    {
        $response = new Response(
            200,
            ['Content-Type' => 'text/plain'],
            '',
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Can\'t process response with content type "text/plain"');

        [...StreamResponse::chunkStreamResponse($response, 30)];
    }

    public function testStreamIsClosedWhenGeneratorIsClosed(): void
    {
        $jsonContent = '{"key1":"value1"}{"key2":"value2"}{"key3":"value3"}';
        $stream = fopen('php://memory', 'r+');
        self::assertIsResource($stream);
        fwrite($stream, $jsonContent);
        rewind($stream);

        $response = new Response(
            200,
            ['Content-Type' => 'application/json'],
            new Stream($stream),
        );

        $generator = StreamResponse::chunkStreamResponse($response, 30);

        // Consume only the first item and then break the iteration
        foreach ($generator as $chunk) {
            self::assertEquals('{"key1":"value1"}', $chunk);
            break;
        }

        // the stream is still opened at this point, something can continue consuming the generator
        self::assertTrue(is_resource($stream));

        unset($generator);

        // here the stream should be closed as the generator was already garbage-collected
        self::assertFalse(is_resource($stream));
    }

    public function testChunkStreamResponseWithTimeout(): void
    {
        $timeout = 2;

        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        self::assertIsArray($sockets);
        $readStream = $sockets[0];

        $response = new Response(
            200,
            ['Content-Type' => 'application/json'],
            new Stream($readStream),
        );

        $startTime = microtime(true);
        foreach (StreamResponse::chunkStreamResponse($response, $timeout) as $chunk) {
            $endTime = microtime(true);
            self::assertEqualsWithDelta($timeout, $endTime - $startTime, 0.1);

            self::assertNull($chunk); // NULL response means no data received (timeout)
            break;
        }
    }

    public function instantiateResponseObjectProvider(): iterable
    {
        yield 'core api pod' => [
            'data' => [
                'apiVersion' => 'v1',
                'kind' => 'Pod',
                'metadata' => [
                    'name' => 'test-pod',
                    'namespace' => 'default',
                ],
            ],
            'expectedClass' => Pod::class,
        ];
    }

    /**
     * @dataProvider instantiateResponseObjectProvider
     * @param class-string $expectedClass
     */
    public function testInstantiateResponseObject(array $data, string $expectedClass): void
    {
        $object = StreamResponse::instantiateResponseObject($data);

        self::assertInstanceOf($expectedClass, $object);
    }

    public function instantiateResponseObjectWithMissingFieldsProvider(): iterable
    {
        yield 'missing apiVersion' => [
            'data' => [
                'kind' => 'Pod',
            ],
        ];

        yield 'missing kind' => [
            'data' => [
                'apiVersion' => 'v1',
            ],
        ];

        yield 'empty data' => [
            'data' => [],
        ];
    }

    /** @dataProvider instantiateResponseObjectWithMissingFieldsProvider */
    public function testInstantiateResponseObjectWithMissingFields(array $data): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Missing apiVersion or kind in object');

        StreamResponse::instantiateResponseObject($data);
    }

    public static function resolveClassForKindProvider(): iterable
    {
        yield 'core api pod' => [
            'apiVersion' => 'v1',
            'kind' => 'Pod',
            'expectedClass' => 'Kubernetes\Model\Io\K8s\Api\Core\V1\Pod',
        ];

        yield 'apps api deployment' => [
            'apiVersion' => 'apps/v1',
            'kind' => 'Deployment',
            'expectedClass' => 'Kubernetes\Model\Io\K8s\Api\Apps\V1\Deployment',
        ];

        yield 'networking api ingress' => [
            'apiVersion' => 'networking.k8s.io/v1',
            'kind' => 'Ingress',
            'expectedClass' => 'Kubernetes\Model\Io\K8s\Api\Networking\V1\Ingress',
        ];
    }

    /** @dataProvider resolveClassForKindProvider */
    public function testResolveClassForKind(string $apiVersion, string $kind, string $expectedClass): void
    {
        $class = StreamResponse::resolveClassForKind($apiVersion, $kind);

        self::assertEquals($expectedClass, $class);
    }

    public function testResolveClassForKindInvalidClass(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Class "Kubernetes\Model\Io\K8s\Api\Core\V1\NonExistentResource" does not exist');

        StreamResponse::resolveClassForKind('v1', 'NonExistentResource');
    }

    public function testResolveClassForKindWithNonAbstractModelClass(): void
    {
        // Create a mock class that exists but is not a subclass of AbstractModel
        $className = 'Kubernetes\Model\Io\K8s\Api\Core\V1\MockNonAbstractModel';
        eval('namespace Kubernetes\Model\Io\K8s\Api\Core\V1; class MockNonAbstractModel {}');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(sprintf('Class "%s" is not "KubernetesRuntime\AbstractModel"', $className));

        StreamResponse::resolveClassForKind('v1', 'MockNonAbstractModel');
    }
}
