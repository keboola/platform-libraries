<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\BaseApi\Data;

use Keboola\K8sClient\BaseApi\Data\WatchEvent;
use Keboola\K8sClient\BaseApi\Data\WatchEventType;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Runtime\RawExtension;
use PHPUnit\Framework\TestCase;

class WatchEventTest extends TestCase
{
    public function fromResponseProvider(): iterable
    {
        yield 'added event' => [
            'data' => [
                'type' => 'ADDED',
                'object' => [
                    'apiVersion' => 'v1',
                    'kind' => 'Pod',
                    'metadata' => [
                        'name' => 'test-pod',
                        'namespace' => 'default',
                    ],
                ],
            ],
            'expectedType' => WatchEventType::Added,
            'expectedObjectClass' => Pod::class,
            'expectedObject' => new Pod([
                'apiVersion' => 'v1',
                'kind' => 'Pod',
                'metadata' => [
                    'name' => 'test-pod',
                    'namespace' => 'default',
                ],
            ]),
        ];

        yield 'modified event' => [
            'data' => [
                'type' => 'MODIFIED',
                'object' => [
                    'apiVersion' => 'v1',
                    'kind' => 'Pod',
                    'metadata' => [
                        'name' => 'test-pod',
                        'namespace' => 'default',
                    ],
                ],
            ],
            'expectedType' => WatchEventType::Modified,
            'expectedObjectClass' => Pod::class,
            'expectedObject' => new Pod([
                'apiVersion' => 'v1',
                'kind' => 'Pod',
                'metadata' => [
                    'name' => 'test-pod',
                    'namespace' => 'default',
                ],
            ]),
        ];

        yield 'deleted event' => [
            'data' => [
                'type' => 'DELETED',
                'object' => [
                    'apiVersion' => 'v1',
                    'kind' => 'Pod',
                    'metadata' => [
                        'name' => 'test-pod',
                        'namespace' => 'default',
                    ],
                ],
            ],
            'expectedType' => WatchEventType::Deleted,
            'expectedObjectClass' => Pod::class,
            'expectedObject' => new Pod([
                'apiVersion' => 'v1',
                'kind' => 'Pod',
                'metadata' => [
                    'name' => 'test-pod',
                    'namespace' => 'default',
                ],
            ]),
        ];

        yield 'error event' => [
            'data' => [
                'type' => 'ERROR',
                'object' => [
                    'code' => 500,
                    'message' => 'Internal server error',
                ],
            ],
            'expectedType' => WatchEventType::Error,
            'expectedObjectClass' => RawExtension::class,
            'expectedObject' => new RawExtension([
                'code' => 500,
                'message' => 'Internal server error',
            ]),
        ];
    }

    /**
     * @dataProvider fromResponseProvider
     * @param class-string $expectedObjectClass
     */
    public function testFromResponse(
        array $data,
        WatchEventType $expectedType,
        string $expectedObjectClass,
        object $expectedObject,
    ): void {
        $event = WatchEvent::fromResponse($data);

        self::assertEquals($expectedType, $event->type);
        self::assertInstanceOf($expectedObjectClass, $event->object);
        self::assertEquals($expectedObject, $event->object);
    }

    public function testConstructor(): void
    {
        $type = WatchEventType::Added;
        $object = new Pod([
            'apiVersion' => 'v1',
            'kind' => 'Pod',
            'metadata' => [
                'name' => 'test-pod',
                'namespace' => 'default',
            ],
        ]);

        $event = new WatchEvent($type, $object);

        self::assertEquals($type, $event->type);
        self::assertSame($object, $event->object);
    }
}
