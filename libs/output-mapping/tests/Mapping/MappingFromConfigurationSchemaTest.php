<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationSchemaTest extends TestCase
{
    public function testMinimalConfig(): void
    {
        $config = [
            'name' => 'col1',
        ];

        $mappingSchema = new MappingFromConfigurationSchemaColumn($config);

        self::assertEquals('col1', $mappingSchema->getName());
        self::assertNull($mappingSchema->getDataType());
        self::assertTrue($mappingSchema->isNullable());
        self::assertFalse($mappingSchema->isPrimaryKey());
        self::assertFalse($mappingSchema->isDistributionKey());
        self::assertEquals([], $mappingSchema->geMetadata());
    }

    public function testFullConfig(): void
    {
        $config = [
            'name' => 'col1',
            'data_type' => [
                'base' => [
                    'type' => 'string',
                    'length' => '1',
                    'default' => 'defaultBase',
                ],
                'snowflake' => [
                    'type' => 'INT',
                    'length' => '2',
                    'default' => 'defaultSnowflake',
                ],
            ],
            'nullable' => false,
            'primary_key' => true,
            'distribution_key' => true,
            'description' => 'col1 description',
            'metadata' => [
                'key1' => 'value1',
                'key2' => 'value2',
            ],
        ];

        $mappingSchema = new MappingFromConfigurationSchemaColumn($config);

        self::assertEquals('col1', $mappingSchema->getName());

        self::assertNotNull($mappingSchema->getDataType());
        self::assertEquals('string', $mappingSchema->getDataType()->getBaseTypeName());
        self::assertEquals('1', $mappingSchema->getDataType()->getBaseLength());
        self::assertEquals('defaultBase', $mappingSchema->getDataType()->getBaseDefaultValue());
        self::assertEquals('INT', $mappingSchema->getDataType()->getBackendTypeName('snowflake'));
        self::assertEquals('2', $mappingSchema->getDataType()->getBackendLength('snowflake'));
        self::assertEquals('defaultSnowflake', $mappingSchema->getDataType()->getBackendDefaultValue('snowflake'));

        self::assertFalse($mappingSchema->isNullable());
        self::assertTrue($mappingSchema->isPrimaryKey());
        self::assertTrue($mappingSchema->isDistributionKey());
        self::assertEquals([
            'key1' => 'value1',
            'key2' => 'value2',
            'KBC.description' => 'col1 description',
        ], $mappingSchema->geMetadata());
    }
}
