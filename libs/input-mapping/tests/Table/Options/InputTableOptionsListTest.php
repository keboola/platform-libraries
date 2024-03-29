<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Options;

use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use PHPUnit\Framework\TestCase;

class InputTableOptionsListTest extends TestCase
{
    public function testGetTables(): void
    {
        $definitions = new InputTableOptionsList([
            ['source' => 'test1'],
            ['source' => 'test2'],
        ]);
        $tables = $definitions->getTables();
        self::assertCount(2, $tables);
        self::assertEquals(InputTableOptions::class, get_class($tables[0]));
        self::assertEquals(InputTableOptions::class, get_class($tables[1]));
        self::assertEquals('test1', $tables[0]->getSource());
        self::assertEquals('test2', $tables[1]->getSource());
    }
}
