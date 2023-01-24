<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\State;

use Keboola\InputMapping\State\InputTableState;
use PHPUnit\Framework\TestCase;

class InputTableStateTest extends TestCase
{
    public function testGetSource(): void
    {
        $state = new InputTableState(['source' => 'test', 'lastImportDate' => '2016-08-31T19:36:00+0200']);
        self::assertEquals('test', $state->getSource());
    }

    public function testGetLastImportDate(): void
    {
        $state = new InputTableState(['source' => 'test', 'lastImportDate' => '2016-08-31T19:36:00+0200']);
        self::assertEquals('2016-08-31T19:36:00+0200', $state->getLastImportDate());
    }

    public function testJsonSerialize(): void
    {
        $configuration = ['source' => 'test', 'lastImportDate' => '2016-08-31T19:36:00+0200'];
        $state = new InputTableState($configuration);
        self::assertEquals($configuration, $state->jsonSerialize());
    }
}
