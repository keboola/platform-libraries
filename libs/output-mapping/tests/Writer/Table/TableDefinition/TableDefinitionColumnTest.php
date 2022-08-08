<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumn;
use PHPUnit\Framework\TestCase;

class TableDefinitionColumnTest extends TestCase
{
    /** @dataProvider createTableDefinitionColumnProvider */
    public function testCreateTableDefinitionColumn(
        string $name,
        ?string $baseType,
        array $expectedSerialisation
    ): void {
        self::assertSame(
            (new TableDefinitionColumn($name, $baseType))->toArray(),
            $expectedSerialisation
        );
    }

    public function createTableDefinitionColumnProvider(): \Generator
    {
        yield [
            'testNoMetadata',
            null,
            [
                'name' => 'testNoMetadata',
                'basetype' => null,
            ],
        ];
        yield [
            'testSnowflakeNative',
            'STRING',
            [
                'name' => 'testSnowflakeNative',
                'basetype' => 'STRING',
            ],
        ];
    }
}
