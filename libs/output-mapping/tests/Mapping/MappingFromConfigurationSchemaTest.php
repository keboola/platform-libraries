<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchema;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationSchemaTest extends TestCase
{
    public function testMinimalConfig(): void
    {
        $config = [
            'name' => 'col1',
        ];

        $mappingSchema = new MappingFromConfigurationSchema($config);

        self::assertEquals('col1', $mappingSchema->getName());
        self::assertNull($mappingSchema->getDataType());
        self::assertTrue($mappingSchema->isNullable());
        self::assertFalse($mappingSchema->isPrimaryKey());
        self::assertFalse($mappingSchema->isDistributionKey());
        self::assertEquals('', $mappingSchema->getDescription());
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
                ]
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

        $mappingSchema = new MappingFromConfigurationSchema($config);

        self::assertEquals('col1', $mappingSchema->getName());

        self::assertNotNull($mappingSchema->getDataType());
        self::assertEquals('string', $mappingSchema->getDataType()->getBaseType());
        self::assertEquals('1', $mappingSchema->getDataType()->getBaseLength());
        self::assertEquals('defaultBase', $mappingSchema->getDataType()->getBaseDefault());
        self::assertEquals('INT', $mappingSchema->getDataType()->getType('snowflake'));
        self::assertEquals('2', $mappingSchema->getDataType()->getLength('snowflake'));
        self::assertEquals('defaultSnowflake', $mappingSchema->getDataType()->getDefault('snowflake'));

        self::assertFalse($mappingSchema->isNullable());
        self::assertTrue($mappingSchema->isPrimaryKey());
        self::assertTrue($mappingSchema->isDistributionKey());
        self::assertEquals('col1 description', $mappingSchema->getDescription());
        self::assertEquals(['key1' => 'value1', 'key2' => 'value2'], $mappingSchema->geMetadata());
    }
}