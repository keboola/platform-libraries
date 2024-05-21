<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaDataType;
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

        $mapping = new MappingFromConfigurationSchemaDataType($config);

        self::assertEquals('string', $mapping->getBaseType());
        self::assertEquals('1', $mapping->getBaseLength());
        self::assertEquals('defaultBase', $mapping->getBaseDefault());

        self::assertEquals('INT', $mapping->getType('snowflake'));
        self::assertEquals('2', $mapping->getLength('snowflake'));
        self::assertEquals('defaultSnowflake', $mapping->getDefault('snowflake'));

        self::assertEquals('TEXT', $mapping->getType('exasol'));
        self::assertEquals('3', $mapping->getLength('exasol'));
        self::assertNull($mapping->getDefault('exasol'));

        self::assertEquals('DATETIME', $mapping->getType('bigquery'));
        self::assertNull($mapping->getLength('bigquery'));
        self::assertEquals('defaultBigQuery', $mapping->getDefault('bigquery'));

        self::assertEquals('VARCHAR', $mapping->getType('teradata'));
        self::assertNull($mapping->getLength('teradata'));
        self::assertNull($mapping->getDefault('teradata'));
    }

    public function testInvalidBackend(): void
    {
        $config = [
            'base' => [
                'type' => 'string',
            ],
        ];

        $mapping = new MappingFromConfigurationSchemaDataType($config);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Backend "snowflake" not found in mapping.');
        $mapping->getType('snowflake');
    }
}
