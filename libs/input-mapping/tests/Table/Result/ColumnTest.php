<?php

namespace Keboola\InputMapping\Tests\Table\Result;

use Keboola\InputMapping\Table\Result\Column;
use Keboola\InputMapping\Table\Result\MetadataItem;
use PHPUnit\Framework\TestCase;

class ColumnTest extends TestCase
{
    public function testAccessors()
    {
        $metadata = [
            [
                'key' => 'key1',
                'value' => 'value1',
                'provider' => 'provider1',
                'timestamp' => 'timestamp1',
            ],
            [
                'key' => 'key2',
                'value' => 'value2',
                'provider' => 'provider2',
                'timestamp' => 'timestamp2',
            ],
        ];
        $column = new Column('my-column', $metadata);
        self::assertSame('my-column', $column->getName());
        self::assertEquals(
            [
                new MetadataItem($metadata[0]),
                new MetadataItem($metadata[1]),
            ],
            iterator_to_array($column->getMetadata())
        );
    }
}
