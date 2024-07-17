<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationSchemaColumnTest extends TestCase
{
    public function testMinimalMappingConfiguration(): void
    {
        $schemColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
        ]);

        self::assertSame('newColumn', $schemColumn->getName());
        self::assertNull($schemColumn->getDataType());
        self::assertTrue($schemColumn->isNullable());
        self::assertFalse($schemColumn->isPrimaryKey());
        self::assertFalse($schemColumn->isDistributionKey());
        self::assertFalse($schemColumn->hasMetadata());
        self::assertSame([], $schemColumn->getMetadata());
    }

    public function testGetters(): void
    {
        $schemColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '255',
                ],
            ],
            'nullable' => false,
            'primary_key' => true,
            'distribution_key' => true,
            'description' => 'Some description of the newColumn.',
            'metadata' => [
                'KBC.datatype.type' => 'STRING',
            ],
        ]);

        self::assertSame('newColumn', $schemColumn->getName());
        self::assertNotNull($schemColumn->getDataType());
        self::assertSame('STRING', $schemColumn->getDataType()->getBaseTypeName());
        self::assertSame('255', $schemColumn->getDataType()->getLength());
        self::assertFalse($schemColumn->isNullable());
        self::assertTrue($schemColumn->isPrimaryKey());
        self::assertTrue($schemColumn->isDistributionKey());
        self::assertTrue($schemColumn->hasMetadata());
        self::assertSame(
            [
                'KBC.datatype.type' => 'STRING',
                'KBC.description' => 'Some description of the newColumn.',
            ],
            $schemColumn->getMetadata(),
        );
    }
}
