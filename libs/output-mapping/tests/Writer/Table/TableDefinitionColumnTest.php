<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Writer\Table\TableDefinitionColumn;
use PHPUnit\Framework\TestCase;

class TableDefinitionColumnTest extends TestCase
{
    /** @dataProvider createTableDefinitionColumnProvider */
    public function testCreateTableDefinitionColumn(string $name, array $metadata, array $expectedSerialisation): void
    {
        self::assertSame(
            (new TableDefinitionColumn($name, $metadata))->toArray(),
            $expectedSerialisation
        );
    }

    public function createTableDefinitionColumnProvider(): \Generator
    {
        yield [
            'testNoMetadata',
            [],
            [
                'name' => 'testNoMetadata',
                'basetype' => null,
                'definition' => null,
            ],
        ];
    }
}
