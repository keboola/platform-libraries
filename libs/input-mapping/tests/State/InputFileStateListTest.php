<?php

namespace Keboola\InputMapping\Tests\State;

use Keboola\InputMapping\Exception\FileNotFoundException;
use Keboola\InputMapping\State\InputFileStateList;
use PHPUnit\Framework\TestCase;

class InputFileStateListTest extends TestCase
{
    public function testGetFile()
    {
        $configuration = [
            [
                'tags' => [
                    [
                        'name' => 'test1',
                        'match' => 'include',
                    ],
                    [
                        'name' => 'test2',
                        'match' => 'exclude',
                    ],
                ],
                'lastImportId' => '12345',
            ],
            [
                'tags' => [
                    [
                        'name' => 'test3',
                        'match' => 'include',
                    ],
                ],
                'lastImportId' => '21323',
            ],
        ];
        $states = new InputFileStateList($configuration);
        self::assertEquals(
            '12345',
            $states->getFile([
                [
                    'name' => 'test1',
                    'match' => 'include',
                ],
                [
                    'name' => 'test2',
                    'match' => 'exclude',
                ],
            ])->getLastImportId()
        );
        self::assertEquals(
            '21323',
            $states->getFile([
                [
                    'name' => 'test3',
                    'match' => 'include',
                ],
            ])->getLastImportId()
        );
    }

    public function testGetFileNotFound()
    {
        $states = new InputFileStateList([]);
        self::expectException(FileNotFoundException::class);
        self::expectExceptionMessage(
            'State for files defined by "[{"name":"test","match":"include"}]" not found.');
        $states->getFile([['name' => 'test', 'match' => 'include']]);
    }

    public function testJsonSerialize()
    {
        $configuration = [
            [
                'tags' => [
                    [
                        'name' => 'test1',
                        'match' => 'include',
                    ],
                    [
                        'name' => 'test2',
                        'match' => 'exclude',
                    ],
                ],
                'lastImportId' => '12345',
            ],
            [
                'tags' => [
                    [
                        'name' => 'test3',
                        'match' => 'include',
                    ],
                ],
                'lastImportId' => '21323',
            ],
        ];
        $states = new InputFileStateList($configuration);
        self::assertEquals($configuration, $states->jsonSerialize());
    }
}
