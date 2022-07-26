<?php

namespace Keboola\OutputMapping\Tests\Writer\Table;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use PHPUnit\Framework\TestCase;

class MappingDestinationTest extends TestCase
{
    public function testValueMustBeString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $value must be a string, boolean given');

        new MappingDestination(false);
    }

    public function testValueMustHaveTableIdFormat()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value is not a valid table ID');

        new MappingDestination('abc');
    }

    public function destinationProvider()
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
     * @param $value
     * @param $tableId
     * @param $bucketId
     * @param $stageId
     * @param $bucketName
     * @param $tableName
     * @dataProvider destinationProvider
     */
    public function testTableIdIsProperlyParsed($value, $tableId, $bucketId, $stageId, $bucketName, $tableName)
    {
        $destination = new MappingDestination($value);
        self::assertSame($tableId, $destination->getTableId());
        self::assertSame($bucketId, $destination->getBucketId());
        self::assertSame($stageId, $destination->getBucketStage());
        self::assertSame($bucketName, $destination->getBucketName());
        self::assertSame($tableName, $destination->getTableName());
    }
}
