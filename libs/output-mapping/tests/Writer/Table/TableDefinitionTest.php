<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Datatype\Definition\Synapse;
use Keboola\OutputMapping\Writer\Table\TableDefinition;
use PHPUnit\Framework\TestCase;

class TableDefinitionTest extends TestCase
{
    /**
     * @dataProvider tableDefinitionConstructorProvider
     */
    public function testTableDefinitionConstructor(
        string $componentId,
        string $bucketType,
        ?string $expectedNativeDatatypeClass
    ): void {
        self::assertSame(
            (new TableDefinition($componentId, $bucketType))->getNativeTypeClass(),
            $expectedNativeDatatypeClass
        );
    }

    public function tableDefinitionConstructorProvider(): \Generator
    {
        yield [
            'keboola.ex-db-snowflake',
            'snowflake',
            Snowflake::class,
        ];
        yield [
            'keboola.synapse-transformation',
            'synapse',
            Synapse::class,
        ];
        yield [
            'keboola.ex-db-mysql',
            'snowflake',
            null,
        ];
        yield [
            'keboola.ex-db-redshift',
            'redshift',
            null,
        ];
        yield [
            'keboola.exasol-transformation',
            'exasol',
            Exasol::class,
        ];
    }

    public function testAddColumn(TableDefinition $definition, string $columnName, array $metadata): void
    {
        $definition->addColumn($columnName, $metadata);
    }

    public function addColumnProvider(): \Generator
    {
        yield [
            new TableDefinition('keboola.ex-db-snowflake', 'snowflake'),
        ];
        yield [
            new TableDefinition('keboola.exasol-transformation', 'exasol'),
        ];
    }
}
