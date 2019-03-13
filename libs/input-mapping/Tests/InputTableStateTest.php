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
        self::assertEquals(new \DateTime('2016-08-31T19:36:00+0200'), $state->getLastImportDate());
        self::assertEquals('2016', $state->getLastImportDate()->format('Y'));
        self::assertEquals('08', $state->getLastImportDate()->format('m'));
        self::assertEquals('31', $state->getLastImportDate()->format('d'));
        self::assertEquals('19', $state->getLastImportDate()->format('H'));
        self::assertEquals('36', $state->getLastImportDate()->format('i'));
        self::assertEquals('00', $state->getLastImportDate()->format('s'));
        self::assertEquals('+02:00', $state->getLastImportDate()->getTimezone()->getName());
    }

    public function testGetLastImportDateInvalidDate()
    {
        self::expectException(InvalidDateException::class);
        self::expectExceptionMessage('Error parsing date "invalid":');
        new InputTableState(['source' => 'test', 'lastImportDate' => 'invalid']);
    }
}
