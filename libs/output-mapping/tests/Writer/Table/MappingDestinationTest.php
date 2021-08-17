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
        $destination = new MappingDestination('in.c-my-bucket.some-table');

        self::assertSame($destination->getTableId(), 'in.c-my-bucket.some-table');
        self::assertSame($destination->getBucketId(), 'in.c-my-bucket');
        self::assertSame($destination->getBucketStage(), 'in');
        self::assertSame($destination->getBucketName(), 'my-bucket');
        self::assertSame($destination->getTableName(), 'some-table');
    }
}
