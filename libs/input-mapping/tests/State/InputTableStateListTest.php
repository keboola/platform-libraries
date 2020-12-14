<?php

namespace Keboola\InputMapping\Tests\State;

use Keboola\InputMapping\Exception\TableNotFoundException;
use Keboola\InputMapping\State\InputTableStateList;
use PHPUnit\Framework\TestCase;

class InputTableStateListTest extends TestCase
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
        $states = new InputTableStateList($configuration);
        self::assertEquals('test', $states->getTable('test')->getSource());
        self::assertEquals('test2', $states->getTable('test2')->getSource());
    }

    public function testGetTableNotFound()
    {
        $states = new InputTableStateList([]);
        self::expectException(TableNotFoundException::class);
        self::expectExceptionMessage('State for table "test" not found.');
        $states->getTable('test');
    }

    public function testJsonSerialize()
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
        $states = new InputTableStateList($configuration);
        self::assertEquals($configuration, $states->jsonSerialize());
    }
}
