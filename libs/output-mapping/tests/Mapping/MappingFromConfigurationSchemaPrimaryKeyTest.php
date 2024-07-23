<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaPrimaryKey;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationSchemaPrimaryKeyTest extends TestCase
{
    public function testPrimaryKey(): void
    {
        $primaryKey = new MappingFromConfigurationSchemaPrimaryKey();
        self::assertEquals([], $primaryKey->getColumns());

        $schemColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'newColumn',
            'primary_key' => true,
        ]);

        $primaryKey->addPrimaryKeyColumn($schemColumn);
        self::assertSame([$schemColumn], $primaryKey->getColumns());
    }
}
