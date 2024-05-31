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

        self::assertEquals('string', $mapping->getBaseTypeName());
        self::assertEquals('1', $mapping->getBaseLength());
        self::assertEquals('defaultBase', $mapping->getBaseDefaultValue());

        self::assertEquals('INT', $mapping->getBackendTypeName('snowflake'));
        self::assertEquals('2', $mapping->getBackendLength('snowflake'));
        self::assertEquals('defaultSnowflake', $mapping->getBackendDefaultValue('snowflake'));

        self::assertEquals('TEXT', $mapping->getBackendTypeName('exasol'));
        self::assertEquals('3', $mapping->getBackendLength('exasol'));
        self::assertNull($mapping->getBackendDefaultValue('exasol'));

        self::assertEquals('DATETIME', $mapping->getBackendTypeName('bigquery'));
        self::assertNull($mapping->getBackendLength('bigquery'));
        self::assertEquals('defaultBigQuery', $mapping->getBackendDefaultValue('bigquery'));

        self::assertEquals('VARCHAR', $mapping->getBackendTypeName('teradata'));
        self::assertNull($mapping->getBackendLength('teradata'));
        self::assertNull($mapping->getBackendDefaultValue('teradata'));
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
        $mapping->getBackendTypeName('snowflake');
    }
}
