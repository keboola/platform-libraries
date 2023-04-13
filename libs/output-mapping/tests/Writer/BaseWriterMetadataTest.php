<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Metadata;

abstract class BaseWriterMetadataTest extends AbstractTestCase
{
    protected string $backend;

    /**
     * Transform metadata into a key-value array
     * @param $metadata
     * @return array
     */
    protected function getMetadataValues(array $metadata): array
    {
        $result = [];
        foreach ($metadata as $item) {
            $result[$item['provider']][$item['key']] = $item['value'];
        }
        return $result;
    }

    protected function metadataWritingTestColumnChangeTest(string $inputBucket, bool $incrementalFlag = false): void
    {
        $root = $this->temp->getTmpFolder();
        $csv = new CsvFile($root . '/table88a.csv');
        $csv->writeRow(['Id', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync($inputBucket, 'table88', $csv);

        $csv = new CsvFile($root . '/upload/table88b.csv');
        $csv->writeRow(['Id', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            'mapping' => [
                [
                    'source' => 'table88b.csv',
                    'destination' => $inputBucket . '.table88',
                    'metadata' => [],
                    'incremental' => $incrementalFlag,
                    'column_metadata' => [
                        'Id' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one id2',
                            ],
                        ],
                        'Name' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one text2',
                            ],
                        ],
                        'Foo' => [
                            [
                                'key' => 'foo.one',
                                'value' => 'bar one',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $runId = $this->clientWrapper->getBasicClient()->generateRunId();
        $this->clientWrapper->getBasicClient()->setRunId($runId);

        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            $config,
            ['componentId' => 'testComponent'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $writerJobs = array_filter(
            $this->clientWrapper->getBasicClient()->listJobs(),
            function (array $job) use ($runId) {
                return $runId === $job['runId'];
            }
        );

        self::assertCount(2, $writerJobs);

        self::assertTableColumnAddJob(array_pop($writerJobs), 'Foo');
        self::assertTableImportJob(array_pop($writerJobs), $incrementalFlag);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $idColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table88.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id2',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $NameColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($NameColMetadata));
        $FooColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($FooColMetadata));
    }

    protected function metadataWritingTestColumnChangeSpecialDelimiter(string $inputBucket): void
    {
        $root = $this->temp->getTmpFolder();
        $csv = new CsvFile($root . '/table88a.csv');
        $csv->writeRow(['Id with special chars', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync($inputBucket, 'table88', $csv);

        $csv = new CsvFile($root . '/upload/table88b.csv', ';', '\'');
        $csv->writeRow(['Id with special chars', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            'mapping' => [
                [
                    'source' => 'table88b.csv',
                    'destination' => $inputBucket . '.table88',
                    'delimiter' => ';',
                    'enclosure' => "'",
                    'metadata' => [],
                    'column_metadata' => [
                        'Name' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one text2',
                            ],
                        ],
                        'Foo' => [
                            [
                                'key' => 'foo.one',
                                'value' => 'bar one',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            $config,
            ['componentId' => 'testComponent'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $nameColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }

    protected function metadataWritingTestColumnChangeSpecialChars(string $inputBucket): void
    {
        $root = $this->temp->getTmpFolder();
        $csv = new CsvFile($root . '/table88a.csv');
        $csv->writeRow(['Id with special chars', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync($inputBucket, 'table88', $csv);

        $csv = new CsvFile($root . '/upload/table88b.csv');
        $csv->writeRow(['Id with special chars', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            'mapping' => [
                [
                    'source' => 'table88b.csv',
                    'destination' => $inputBucket . '.table88',
                    'metadata' => [],
                    'column_metadata' => [
                        'Id with special chars' => [
                            [
                                'key' => 'column.key.zero',
                                'value' => 'column value on id',
                            ],
                        ],
                        'Name' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one text2',
                            ],
                        ],
                        'Foo' => [
                            [
                                'key' => 'foo.one',
                                'value' => 'bar one',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            $config,
            ['componentId' => 'testComponent'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $idColMetadata = $metadataApi->listColumnMetadata(
            $inputBucket . '.table88.Id_with_special_chars'
        );
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.zero' => 'column value on id',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $nameColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }

    protected function metadataWritingTestColumnChangeHeadless(string $inputBucket): void
    {
        $root = $this->temp->getTmpFolder();
        $csv = new CsvFile($root . '/table99a.csv');
        $csv->writeRow(['Id', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync($inputBucket, 'table99', $csv);

        mkdir($root . '/upload/table99b', 0777, true);
        $csv = new CsvFile($root . '/upload/table99b/slice1.csv');
        $csv->writeRow(['test', 'test', 'bar']);
        $csv = new CsvFile($root . '/upload/table99b/slice2.csv');
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            'mapping' => [
                [
                    'source' => 'table99b',
                    'destination' => $inputBucket . '.table99',
                    'columns' => ['Id', 'Name', 'Foo'],
                    'metadata' => [],
                    'column_metadata' => [
                        'Id' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one id2',
                            ],
                        ],
                        'Name' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one text2',
                            ],
                        ],
                        'Foo' => [
                            [
                                'key' => 'foo.one',
                                'value' => 'bar one',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $writer = new TableWriter($this->getLocalStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            $config,
            ['componentId' => 'testComponent'],
            AbstractStrategyFactory::LOCAL,
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $idColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table99.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id2',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $nameColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table99.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata($inputBucket . '.table99.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }
}
