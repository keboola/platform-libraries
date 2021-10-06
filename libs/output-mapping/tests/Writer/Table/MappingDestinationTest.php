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

    public function testTableIdIsProperlyParsed()
    {
        // c- prefixed
        $destination = new MappingDestination('in.c-my-bucket.some-table');
        self::assertSame('in.c-my-bucket.some-table', $destination->getTableId());
        self::assertSame('in.c-my-bucket', $destination->getBucketId());
        self::assertSame('in', $destination->getBucketStage());
        self::assertSame('my-bucket', $destination->getBucketName());
        self::assertSame('some-table', $destination->getTableName());

        // non-prefixed
        $destination = new MappingDestination('in.my-bucket.some-table');
        self::assertSame('in.my-bucket.some-table', $destination->getTableId());
        self::assertSame('in.my-bucket', $destination->getBucketId());
        self::assertSame('in', $destination->getBucketStage());
        self::assertSame('my-bucket', $destination->getBucketName());
        self::assertSame('some-table', $destination->getTableName());

        // make sure buckets starting with 'c' are not broken
        $destination = new MappingDestination('in.c-clever-bucket.some-table');
        self::assertSame('in.c-clever-bucket', $destination->getBucketId());
        self::assertSame('in', $destination->getBucketStage());
        self::assertSame('clever-bucket', $destination->getBucketName());
    }
}
