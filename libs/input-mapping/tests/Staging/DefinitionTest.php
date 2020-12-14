<?php

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Staging\Definition;
use PHPUnit\Framework\TestCase;

class DefinitionTest extends TestCase
{
    public function testAccessors()
    {
        $definition = new Definition('foo', 'bar', 'kochba');
        self::assertSame('foo', $definition->getName());
        self::assertSame('bar', $definition->getFileStagingClass());
        self::assertSame('kochba', $definition->getTableStagingClass());
        self::assertNull($definition->getFileDataProvider());
        self::assertSame($definition->getFileDataProvider());
        self::assertSame($definition->getFileDataProvider());
        self::assertSame($definition->getFileDataProvider());
    }
}
