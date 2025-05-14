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
            'bigquery' => [
                'type' => 'DATETIME',
                'default' => 'defaultBigQuery',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaColumnDataType($config);

        self::assertEquals('string', $mapping->getBaseTypeName());
        self::assertEquals('1', $mapping->getBaseLength());
        self::assertEquals('defaultBase', $mapping->getBaseDefaultValue());

        self::assertEquals('INT', $mapping->getBackendTypeName('snowflake'));
        self::assertEquals('2', $mapping->getBackendLength('snowflake'));
        self::assertEquals('defaultSnowflake', $mapping->getBackendDefaultValue('snowflake'));

        self::assertEquals('DATETIME', $mapping->getBackendTypeName('bigquery'));
        self::assertNull($mapping->getBackendLength('bigquery'));
        self::assertEquals('defaultBigQuery', $mapping->getBackendDefaultValue('bigquery'));

        self::assertTrue($mapping->hasBackendType('snowflake'));
    }

    public function testInvalidBackend(): void
    {
        $config = [
            'base' => [
                'type' => 'string',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaColumnDataType($config);

        self::assertFalse($mapping->hasBackendType('snowflake'));

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Backend "snowflake" not found in mapping.');
        $mapping->getBackendTypeName('snowflake');
    }

    public function testDecideTypeName(): void
    {
        $config = [
            'base' => [
                'type' => 'string',
            ],
            'snowflake' => [
                'type' => 'INT',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaColumnDataType($config);
        self::assertEquals('INT', $mapping->getTypeName('snowflake'));
        self::assertEquals('string', $mapping->getTypeName('bigquery'));
        self::assertEquals('string', $mapping->getTypeName());
    }

    public function testDecideLength(): void
    {
        $config = [
            'base' => [
                'type' => 'string',
                'length' => '1',
            ],
            'snowflake' => [
                'type' => 'INT',
                'length' => '2',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaColumnDataType($config);
        self::assertEquals('2', $mapping->getLength('snowflake'));
        self::assertEquals('1', $mapping->getLength('bigquery'));
        self::assertEquals('1', $mapping->getLength());
    }

    public function testDecideDefaultValue(): void
    {
        $config = [
            'base' => [
                'type' => 'string',
                'default' => 'defaultBase',
            ],
            'snowflake' => [
                'type' => 'INT',
                'default' => 'defaultSnowflake',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaColumnDataType($config);
        self::assertEquals('defaultSnowflake', $mapping->getDefaultValue('snowflake'));
        self::assertEquals('defaultBase', $mapping->getDefaultValue('bigquery'));
        self::assertEquals('defaultBase', $mapping->getDefaultValue());
    }
}
