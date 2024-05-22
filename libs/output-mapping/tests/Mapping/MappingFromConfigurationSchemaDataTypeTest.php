<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumnDataType;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationSchemaDataTypeTest extends TestCase
{
    public function testValidConfig(): void
    {
        $config = [
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
            'exasol' => [
                'type' => 'TEXT',
                'length' => '3',
            ],
            'bigquery' => [
                'type' => 'DATETIME',
                'default' => 'defaultBigQuery',
            ],
            'teradata' => [
                'type' => 'VARCHAR',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaColumnDataType($config);

        self::assertEquals('string', $mapping->getBaseType());
        self::assertEquals('1', $mapping->getBaseLength());
        self::assertEquals('defaultBase', $mapping->getBaseDefault());

        self::assertEquals('INT', $mapping->getTypeName('snowflake'));
        self::assertEquals('2', $mapping->getLength('snowflake'));
        self::assertEquals('defaultSnowflake', $mapping->getDefaultValue('snowflake'));

        self::assertEquals('TEXT', $mapping->getTypeName('exasol'));
        self::assertEquals('3', $mapping->getLength('exasol'));
        self::assertNull($mapping->getDefaultValue('exasol'));

        self::assertEquals('DATETIME', $mapping->getTypeName('bigquery'));
        self::assertNull($mapping->getLength('bigquery'));
        self::assertEquals('defaultBigQuery', $mapping->getDefaultValue('bigquery'));

        self::assertEquals('VARCHAR', $mapping->getTypeName('teradata'));
        self::assertNull($mapping->getLength('teradata'));
        self::assertNull($mapping->getDefaultValue('teradata'));
    }

    public function testInvalidBackend(): void
    {
        $config = [
            'base' => [
                'type' => 'string',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaColumnDataType($config);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Backend "snowflake" not found in mapping.');
        $mapping->getTypeName('snowflake');
    }
}
