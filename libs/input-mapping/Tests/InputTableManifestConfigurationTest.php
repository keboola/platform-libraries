<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Configuration\Table\Manifest;

class InputTableManifestConfigurationTest extends \PHPUnit_Framework_TestCase
{
    public function testConfiguration()
    {
        $config = [
            "id" => "in.c-docker-test.test",
            "uri" => "https://connection.keboola.com//v2/storage/tables/in.c-docker-test.test",
            "name" => "test",
            "primary_key" => ["col1", "col2"],
            "indexed_columns" => ["col1", "col2"],
            "created" => "2015-01-23T04:11:18+0100",
            "last_import_date" => "2015-01-23T04:11:18+0100",
            "last_change_date" => "2015-01-23T04:11:18+0100",
            "rows_count" => 100,
            "data_size_bytes" => 32768,
            "is_alias" => false,
            "columns" => ["col1", "col2", "col3", "col4"],
            "attributes" => [["name" => "test", "value" => "test", "protected" => false]],
            "metadata" => [[
                "key" => "foo",
                "value" => "bar",
                "id" => 1234,
                "provider" => "dummy-component",
                "timestamp" => "2017-05-25T16:12:02+0200"
            ]],
            "column_metadata" => ["col1" => [["key" => "bar", "value" => "baz"]]]
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new Manifest())->parse(["config" => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testConfigurationAlias()
    {
        $config = [
            "id" => "in.c-docker-test.test",
            "uri" => "https://connection.keboola.com//v2/storage/tables/in.c-docker-test.test",
            "name" => "test",
            "primary_key" => ["col1", "col2"],
            "indexed_columns" => ["col1", "col2"],
            "created" => "2015-01-23T04:11:18+0100",
            "last_import_date" => "2015-01-23T04:11:18+0100",
            "last_change_date" => "2015-01-23T04:11:18+0100",
            "rows_count" => 0,
            "data_size_bytes" => 0,
            "is_alias" => true,
            "columns" => ["col1", "col2", "col3", "col4"],
            "attributes" => [["name" => "test", "value" => "test", "protected" => false]]
        ];
        $expectedResponse = $config;
        $expectedResponse["metadata"] = [];
        $expectedResponse["column_metadata"] = [];
        $processedConfiguration = (new Manifest())->parse(["config" => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage The child node "id" at path "table" must be configured.
     */
    public function testEmptyConfiguration()
    {
        (new Manifest())->parse(["config" => []]);
    }
}
