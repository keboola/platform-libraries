<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Generator;
use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use PHPUnit\Framework\TestCase;

class MappingDestinationTest extends TestCase
{
    public function testValueMustHaveTableIdFormat(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value is not a valid table ID');

        new MappingDestination('abc');
    }

    public function destinationProvider(): Generator
    {
        yield 'c-prefixed' => [
            'value' => 'in.c-my-bucket.some-table',
            'tableId' => 'in.c-my-bucket.some-table',
            'bucketId' => 'in.c-my-bucket',
            'stageId' => 'in',
            'bucketName' => 'my-bucket',
            'tableName' => 'some-table',
        ];
        yield 'non-prefixed' => [
            'value' => 'in.c-my-bucket.some-table',
            'tableId' => 'in.c-my-bucket.some-table',
            'bucketId' => 'in.c-my-bucket',
            'stageId' => 'in',
            'bucketName' => 'my-bucket',
            'tableName' => 'some-table',
        ];
        yield 'name starts with c' => [
            'value' => 'in.c-clever-bucket.some-table',
            'tableId' => 'in.c-clever-bucket.some-table',
            'bucketId' => 'in.c-clever-bucket',
            'stageId' => 'in',
            'bucketName' => 'clever-bucket',
            'tableName' => 'some-table',
        ];
    }

    /**
     * @dataProvider destinationProvider
     */
    public function testTableIdIsProperlyParsed(
        string $value,
        string $tableId,
        string $bucketId,
        string $stageId,
        string $bucketName,
        string $tableName
    ): void {
        $destination = new MappingDestination($value);
        self::assertSame($tableId, $destination->getTableId());
        self::assertSame($bucketId, $destination->getBucketId());
        self::assertSame($stageId, $destination->getBucketStage());
        self::assertSame($bucketName, $destination->getBucketName());
        self::assertSame($tableName, $destination->getTableName());
    }
}
