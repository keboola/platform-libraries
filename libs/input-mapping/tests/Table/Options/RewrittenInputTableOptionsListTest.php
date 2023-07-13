<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Options;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptionsList;
use PHPUnit\Framework\TestCase;

class RewrittenInputTableOptionsListTest extends TestCase
{
    public function testGetTables(): void
    {
        $definitions = new RewrittenInputTableOptionsList([
            new RewrittenInputTableOptions(['source' => 'test1'], 'test1', 1, ['a' => 'b']),
            new RewrittenInputTableOptions(['source' => 'test2'], 'test2', 2, ['c' => 'd']),
        ]);
        $tables = $definitions->getTables();
        self::assertCount(2, $tables);
        self::assertEquals(RewrittenInputTableOptions::class, get_class($tables[0]));
        self::assertEquals(RewrittenInputTableOptions::class, get_class($tables[1]));
        self::assertEquals('test1', $tables[0]->getSource());
        self::assertEquals('test2', $tables[1]->getSource());
        self::assertEquals(1, $tables[0]->getSourceBranchId());
        self::assertEquals(2, $tables[1]->getSourceBranchId());
        self::assertEquals(['a' => 'b'], $tables[0]->getTableInfo());
        self::assertEquals(['c' => 'd'], $tables[1]->getTableInfo());
    }
}
