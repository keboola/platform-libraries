<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Reader\State\Exception\InvalidDateException;
use Keboola\InputMapping\Reader\State\InputTableState;

class InputTableStateTest extends \PHPUnit_Framework_TestCase
{
    public function testGetSource()
    {
        $state = new InputTableState(['source' => 'test', 'lastImportDate' => '2016-08-31T19:36:00+0200']);
        self::assertEquals('test', $state->getSource());
    }

    public function testGetLastImportDate()
    {
        $state = new InputTableState(['source' => 'test', 'lastImportDate' => '2016-08-31T19:36:00+0200']);
        self::assertEquals('2016-08-31T19:36:00+0200', $state->getLastImportDate());
    }

    public function testJsonSerialize()
    {
        $configuration = ['source' => 'test', 'lastImportDate' => '2016-08-31T19:36:00+0200'];
        $state = new InputTableState($configuration);
        self::assertEquals($configuration, $state->jsonSerialize());
    }
}
