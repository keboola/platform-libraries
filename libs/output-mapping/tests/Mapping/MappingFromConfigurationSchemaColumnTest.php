<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;
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
        self::assertNull($schemColumn->getDescription());
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
        // the description is stored in the native Storage description field, not as KBC.description metadata
        self::assertSame(['KBC.datatype.type' => 'STRING'], $schemColumn->getMetadata());
        self::assertSame('Some description of the newColumn.', $schemColumn->getDescription());
    }

    public function testGetDescriptionFromMetadata(): void
    {
        $schemColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'metadata' => [
                'KBC.description' => 'Description from metadata.',
            ],
        ]);

        self::assertSame('Description from metadata.', $schemColumn->getDescription());
        // the key is consumed as the description, so it must not be reported as metadata as well
        self::assertSame([], $schemColumn->getMetadata());
        self::assertFalse($schemColumn->hasMetadata());
    }

    /**
     * The node is a variableNode, so a non-object value reaches the code. Dropping the metadata silently would
     * hide the configuration error, so it is reported instead.
     */
    public function testNonObjectMetadataIsReported(): void
    {
        $schemColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'metadata' => 'this is a variableNode, so it may be anything',
        ]);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Configuration node "schema.metadata" must be an object, "string" given.');

        $schemColumn->getDescription();
    }
}
