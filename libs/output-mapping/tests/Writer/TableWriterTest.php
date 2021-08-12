<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\TableWriter;

class TableWriterTest extends BaseWriterTest
{
    /**
     * @dataProvider provideDestinationConfigurations
     */
    public function testAllowedDestinationConfigurations($manifest, $defaultBucket, $mapping, $expectedError, $expectedTable)
    {
        $root = $this->tmp->getTmpFolder() . '/upload/';

        file_put_contents($root . 'table.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        if ($manifest !== null) {
            file_put_contents($root . 'table.csv.manifest', json_encode($manifest));
        }

        $tableWriter = new TableWriter($this->getStagingFactory());

        if ($expectedError !== null) {
            $this->expectException(InvalidOutputException::class);
            $this->expectExceptionMessage($expectedError);
        }

        $queue = $tableWriter->uploadTables('upload', ['bucket' => $defaultBucket, 'mapping' => $mapping], ['componentId' => 'foo'], 'local');
        $queue->waitForAll();

        $tables = $this->clientWrapper->getBasicClient()->listTables('out.c-output-mapping-test');
        self::assertCount(1, $tables);
        self::assertSame($expectedTable, $tables[0]['id']);
    }

    public function provideDestinationConfigurations()
    {
        return [
            'table ID nowhere' => [
                'manifest' => null,
                'defaultBucket' => 'out.c-output-mapping-test',
                'mapping' => null,
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
                'expectedTable' => null,
            ],

            'table ID in mapping is accepted' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'out.c-output-mapping-test.table']
                ],
                'expectedError' => null,
                'expectedTable' => 'out.c-output-mapping-test.table',
            ],

            'table name in mapping is not accepted' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'table']
                ],
                'expectedError' => 'Failed to resolve valid destination. "table" is not a valid table ID.',
                'expectedTable' => null,
            ],

            'table name in mapping does not combine with default bucket' => [
                'manifest' => null,
                'defaultBucket' => 'out.c-output-mapping-test',
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'table']
                ],
                'expectedError' => 'Failed to resolve valid destination. "table" is not a valid table ID.',
                'expectedTable' => null,
            ],

            'table ID in manifest is accepted' => [
                'manifest' => ['destination' => 'out.c-output-mapping-test.table'],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedError' => null,
                'expectedTable' => 'out.c-output-mapping-test.table',
            ],

            'table name in manifest without bucket is not accepted' => [
                'manifest' => ['destination' => 'table'],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedError' => 'Failed to resolve valid destination. "table" is not a valid table ID.',
                'expectedTable' => null,
            ],

            'table name in manifest with bucket is accepted' => [
                'manifest' => ['destination' => 'table'],
                'defaultBucket' => 'out.c-output-mapping-test',
                'mapping' => null,
                'expectedError' => null,
                'expectedTable' => 'out.c-output-mapping-test.table',
            ],

            'table ID in mapping overrides manifest' => [
                'manifest' => ['destination' => 'out.c-output-mapping-test.table'],
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'out.c-output-mapping-test.table1']
                ],
                'expectedError' => null,
                'expectedTable' => 'out.c-output-mapping-test.table1',
            ],
        ];
    }

    public function testLocalTableUploadRequiresComponentId()
    {
        $tableWriter = new TableWriter($this->getStagingFactory());

        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');

        $tableWriter->uploadTables('upload', [], [], 'local');
    }

    public function testLocalTableUploadChecksForOrphanedManifests()
    {
        $root = $this->tmp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'table.csv.manifest', json_encode([]));

        $tableWriter = new TableWriter($this->getStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "table.csv.manifest"');

        $tableWriter->uploadTables('upload', [], ['componentId' => 'foo'], 'local');
    }

    public function testLocalTableUploadChecksForUnusedMappingEntries()
    {
        $tableWriter = new TableWriter($this->getStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table source "unknown.csv" not found.');

        $tableWriter->uploadTables('upload', [
            'mapping' => [
                [
                    'source' => 'unknown.csv',
                    'destination' => 'unknown',
                ]
            ]
        ], ['componentId' => 'foo'], 'local');
    }
}
