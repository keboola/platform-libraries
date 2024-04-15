<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Sandboxes\Legacy;

use Generator;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\PersistentStorage;
use PHPUnit\Framework\TestCase;

class PersistentStorageTest extends TestCase
{
    /** @dataProvider fromArrayProvider */
    public function testFromArray(array $data, PersistentStorage $expectedValue): void
    {
        $persistentStorage = PersistentStorage::fromArray($data);
        self::assertEquals($expectedValue, $persistentStorage);
    }

    public function fromArrayProvider(): Generator
    {
        yield 'empty' => [
            [],
            PersistentStorage::create(),
        ];

        yield 'readyTrue' => [
            ['ready' => true],
            PersistentStorage::create()->setReady(true),
        ];

        yield 'readyFalse' => [
            ['ready' => false],
            PersistentStorage::create()->setReady(false),
        ];

        yield 'readyNull' => [
            ['ready' => null],
            PersistentStorage::create(),
        ];

        yield 'with k8sStorageClass' => [
            ['k8sStorageClassName' => 'storage-class'],
            PersistentStorage::create()->setK8sStorageClassName('storage-class'),
        ];

        yield 'all' => [
            [
                'ready' => true,
                'k8sStorageClassName' => 'storage-class',
            ],
            PersistentStorage::create()->setReady(true)->setK8sStorageClassName('storage-class'),
        ];
    }

    /** @dataProvider toArrayProvider */
    public function testToArray(PersistentStorage $persistentStorage, array $expectedValue): void
    {
        self::assertSame($expectedValue, $persistentStorage->toArray());
    }

    public function toArrayProvider(): Generator
    {
        yield 'empty' => [
            PersistentStorage::create(),
            [
                'ready' => null,
            ],
        ];

        yield 'readyTrue' => [
            PersistentStorage::create()->setReady(true),
            [
                'ready' => true,
            ],
        ];

        yield 'readyFalse' => [
            PersistentStorage::create()->setReady(false),
            [
                'ready' => false,
            ],
        ];

        yield 'readyNull' => [
            PersistentStorage::create()->setReady(null),
            [
                'ready' => null,
            ],
        ];

        yield 'with k8sStorageClass' => [
            PersistentStorage::create()->setK8sStorageClassName('storage-class'),
            [
                'ready' => null,
                'k8sStorageClassName' => 'storage-class',
            ],
        ];

        yield 'all' => [
            PersistentStorage::create()->setReady(true)->setK8sStorageClassName('storage-class'),
            [
                'ready' => true,
                'k8sStorageClassName' => 'storage-class',
            ],
        ];
    }

    public function testSetIsReady(): void
    {
        $persistentStorage = new PersistentStorage();
        self::assertNull($persistentStorage->isReady());

        $persistentStorage->setReady(true);
        self::assertTrue($persistentStorage->isReady());

        $persistentStorage->setReady(false);
        self::assertFalse($persistentStorage->isReady());

        $persistentStorage->setReady(null);
        self::assertNull($persistentStorage->isReady());
    }

    public function testSetK8sStorageClassName(): void
    {
        $persistentStorage = new PersistentStorage();
        self::assertSame('', $persistentStorage->getK8sStorageClassName());

        $persistentStorage->setK8sStorageClassName('storage-class');
        self::assertSame('storage-class', $persistentStorage->getK8sStorageClassName());

        $persistentStorage->setK8sStorageClassName(null);
        self::assertNull($persistentStorage->getK8sStorageClassName());
    }
}
