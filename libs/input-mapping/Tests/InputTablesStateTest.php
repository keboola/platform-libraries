<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Reader\State\Exception\TableNotFoundException;
use Keboola\InputMapping\Reader\State\InputTablesState;

class InputTablesStateTest extends \PHPUnit_Framework_TestCase
{
    public function testGetTable()
    {
        $configuration = [
            [
                'source' => 'test',
                'lastImportDate' => '2016-08-31T19:36:00+0200',
            ],
            [
                'source' => 'test2',
                'lastImportDate' => '2016-08-30T19:36:00+0200',
            ]
        ];
        $states = new InputTablesState($configuration);
        self::assertEquals('test', $states->getTable('test')->getSource());
        self::assertEquals('test2', $states->getTable('test2')->getSource());
    }

    public function testGetTableNotFound()
    {
        $states = new InputTablesState([]);
        self::expectException(TableNotFoundException::class);
        self::expectExceptionMessage('State for table "test" not found.');
        $states->getTable('test');
    }

    public function testToArray()
    {
        $configuration = [
            [
                'source' => 'test',
                'lastImportDate' => '2016-08-31T19:36:00+0200',
            ],
            [
                'source' => 'test2',
                'lastImportDate' => '2016-08-30T19:36:00+0200',
            ]
        ];
        $states = new InputTablesState($configuration);
        self::assertEquals($configuration, $states->toArray());
    }
}
