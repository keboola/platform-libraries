<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\MySQL;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinitionColumn;
use PHPUnit\Framework\TestCase;

class TableDefinitionColumnTest extends TestCase
{
    /** @dataProvider createTableDefinitionColumnProvider */
    public function testCreateTableDefinitionColumn(
        string $name,
        array $metadata,
        ?string $nativeTypeClass,
        array $expectedSerialisation
    ): void {
        self::assertSame(
            (new TableDefinitionColumn($name, $metadata, $nativeTypeClass))->toArray(),
            $expectedSerialisation
        );
    }

    public function createTableDefinitionColumnProvider(): \Generator
    {
        yield [
            'testNoMetadata',
            [],
            null,
            [
                'name' => 'testNoMetadata',
                'basetype' => null,
                'definition' => null,
            ],
        ];
        yield [
            'testSnowflakeNative',
            [
                [
                    'key' => Common::KBC_METADATA_KEY_TYPE,
                    'value' => 'VARIANT',
                ],
                [
                    'key' => Common::KBC_METADATA_KEY_BASETYPE,
                    'value' => 'STRING',
                ],
                [
                    'key' => Common::KBC_METADATA_KEY_NULLABLE,
                    'value' => false,
                ],
            ],
            Snowflake::class,
            [
                'name' => 'testSnowflakeNative',
                'basetype' => 'STRING',
                'definition' => (new Snowflake('VARIANT', ['nullable' => false]))->toArray(),
            ],
        ];
        yield [
            'testMetadataFromDifferentSource',
            [
                [
                    'key' => Common::KBC_METADATA_KEY_TYPE,
                    'value' => Exasol::TYPE_CLOB
                ],
                [
                    'key' => Common::KBC_METADATA_KEY_BASETYPE,
                    'value' => 'STRING',
                ],
                [
                    'key' => Common::KBC_METADATA_KEY_NULLABLE,
                    'value' => false,
                ],
            ],
            Snowflake::class,
            [
                'name' => 'testSnowflakeNative',
                'basetype' => 'STRING',
                'definition' => (new Snowflake('VARIANT', ['nullable' => false]))->toArray(),
            ],
        ];
    }
}
