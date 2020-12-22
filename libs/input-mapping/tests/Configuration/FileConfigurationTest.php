<?php

namespace Keboola\InputMapping\Tests\Configuration;

use Keboola\InputMapping\Configuration\File;

class FileConfigurationTest extends \PHPUnit_Framework_TestCase
{
    public function testConfiguration()
    {
        $config = [
            "tags" => ["tag1", "tag2"],
            "query" => "esquery",
            "processed_tags" => ["tag3"],
            "filter_by_run_id" => true,
            "limit" => 1000
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(["config" => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testEmptyTagsRemoved()
    {
        $config = [
            "tags" => [],
            "query" => "esquery",
            "processed_tags" => ["tag3"],
            "filter_by_run_id" => true,
            "limit" => 1000,
        ];
        $expectedResponse = $config;
        unset($expectedResponse["tags"]);
        $processedConfiguration = (new File())->parse([
            "config" => $config,
        ]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testEmptyProcessedTagsRemoved()
    {
        $config = [
            "tags" => ["tag3"],
            "query" => "esquery",
            "processed_tags" => [],
            "filter_by_run_id" => true,
            "limit" => 1000,
        ];
        $expectedResponse = $config;
        unset($expectedResponse["processed_tags"]);
        $processedConfiguration = (new File())->parse([
            "config" => $config,
        ]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testEmptyQueryRemoved()
    {
        $config = [
            "tags" => ["tag1"],
            "query" => "",
            "processed_tags" => ["tag3"],
            "filter_by_run_id" => true,
            "limit" => 1000,
        ];
        $expectedResponse = $config;
        unset($expectedResponse["query"]);
        $processedConfiguration = (new File())->parse([
            "config" => $config,
        ]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testConfigurationWithSourceTags()
    {
        $config = [
            "query" => "esquery",
            "processed_tags" => ["tag3"],
            "filter_by_run_id" => true,
            "limit" => 1000,
            "source" => [
                "tags" => [
                    [
                        "name" => "tag1"
                    ],
                    [
                        "name" => "tag2"
                    ]
                ]
            ],
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(["config" => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "file": At least one of 'tags', 'source.tags' or 'query' parameters must be defined.
     */
    public function testEmptyConfiguration()
    {
        (new File())->parse(["config" => []]);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "file": Both 'tags' and 'source.tags' cannot be defined.
     */
    public function testConfigurationWithTagsAndSourceTags()
    {
        (new File())->parse(["config" => [
            "tags" => ["tag1"],
            "source" => [
                "tags" => [
                    [
                        "name" => "tag1"
                    ],
                    [
                        "name" => "tag2"
                    ]
                ]
            ],
        ]]);
    }
}
