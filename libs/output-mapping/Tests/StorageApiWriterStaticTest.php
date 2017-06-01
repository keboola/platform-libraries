<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Writer\Writer;

class StorageApiWriterStaticTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @dataProvider modifyPrimaryKeyDeciderOptionsProvider
     * @param array $tableInfo
     * @param array $config
     * @param $result
     */
    public function testModifyPrimaryKeyDecider(array $tableInfo, array $config, $result)
    {
        $this->assertEquals($result, Writer::modifyPrimaryKeyDecider($tableInfo, $config));
    }

    /**
     * @dataProvider normalizePrimaryKeyProvider
     * @param array $pkey
     * @param array $result
     */
    public function testNormalizePrimaryKey(array $pkey, array $result)
    {
        $this->assertEquals($result, Writer::normalizePrimaryKey($pkey));
    }

    /**
     * @return array
     */
    public function modifyPrimaryKeyDeciderOptionsProvider()
    {
        return [
            [
                [
                    "primaryKey" => []
                ],
                [
                    "primary_key" => []
                ],
                false
            ],
            [
                [
                    "primaryKey" => []
                ],
                [
                    "primary_key" => ["Id"]
                ],
                true
            ],
            [
                [
                    "primaryKey" => ["Id"]
                ],
                [
                    "primary_key" => []
                ],
                true
            ],
            [
                [
                    "primaryKey" => ["Id"]
                ],
                [
                    "primary_key" => ["Id"]
                ],
                false
            ],
            [
                [
                    "primaryKey" => ["Id"]
                ],
                [
                    "primary_key" => ["Name"]
                ],
                true
            ],
            [
                [
                    "primaryKey" => ["Id"]
                ],
                [
                    "primary_key" => ["Id", "Name"]
                ],
                true
            ],
        ];
    }

    /**
     * @return array
     */
    public function normalizePrimaryKeyProvider()
    {
        return [
            [
                [""],
                []
            ],
            [
                [""],
                []
            ],
            [
                ["Id", "Id"],
                ["Id"]
            ],
            [
                ["Id", "Name"],
                ["Id", "Name"]
            ]
        ];
    }
}
