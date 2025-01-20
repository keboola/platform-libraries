<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Helper\FakeDevStorageTableRewriteHelper;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\Branch;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class FakeDevStorageTableRewriteHelperTest extends AbstractTestCase
{
    #[NeedsEmptyInputBucket, NeedsEmptyOutputBucket, NeedsDevBranch]
    public function testNoBranch(): void
    {
        $clientWrapper = $this->initClient();
        $this->initEmptyFakeBranchInputBucket($clientWrapper);
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        // Create table
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyOutputBucketId,
            'my-table',
            $csv,
        );
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyInputBucketId,
            'my-table-2',
            $csv,
        );

        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->emptyOutputBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => $this->emptyInputBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = (new FakeDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertEquals(
            $this->emptyOutputBucketId . '.my-table',
            $destinations->getTables()[0]->getSource(),
        );
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => $this->emptyOutputBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
                'column_types' => [
                    ['source' => 'id'],
                    ['source' => 'name'],
                ],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
        self::assertEquals($this->emptyInputBucketId . '.my-table-2', $destinations->getTables()[1]->getSource());
        self::assertEquals('my-table-2.csv', $destinations->getTables()[1]->getDestination());
        self::assertEquals(
            [
                'source' => $this->emptyInputBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
                'column_types' => [
                    ['source' => 'foo'],
                    ['source' => 'bar'],
                ],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[1]->getDefinition(),
        );
    }

    #[NeedsEmptyInputBucket, NeedsDevBranch]
    public function testInvalidName(): void
    {
        $clientWrapper = $this->initClient($this->devBranchId);

        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->emptyInputBucketId,
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
        ]);
        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('Invalid destination: "' . $this->emptyInputBucketId . '"');
        (new FakeDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
    }

    #[NeedsEmptyInputBucket, NeedsDevBranch]
    public function testBranchRewriteNoTables(): void
    {
        $clientWrapper = $this->initClient();
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        // Create table
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyInputBucketId,
            'my-table',
            $csv,
        );
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyInputBucketId,
            'my-table-2',
            $csv,
        );

        $clientWrapper = $this->initClient($this->devBranchId);
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->emptyInputBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => $this->emptyInputBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = (new FakeDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertEquals($this->emptyInputBucketId. '.my-table', $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => $this->emptyInputBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
                'column_types' => [
                    ['source' => 'id'],
                    ['source' => 'name'],
                ],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
        self::assertEquals($this->emptyInputBucketId . '.my-table-2', $destinations->getTables()[1]->getSource());
        self::assertEquals('my-table-2.csv', $destinations->getTables()[1]->getDestination());
        self::assertEquals(
            [
                'source' => $this->emptyInputBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
                'column_types' => [
                    ['source' => 'foo'],
                    ['source' => 'bar'],
                ],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[1]->getDefinition(),
        );
    }

    #[NeedsEmptyInputBucket, NeedsDevBranch]
    public function testBranchRewriteTablesExists(): void
    {
        $this->initEmptyFakeBranchInputBucket($this->initClient());

        $clientWrapper = $this->initClient($this->devBranchId);
        file_put_contents($this->temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($this->temp->getTmpFolder() . 'data.csv');

        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyBranchInputBucketId,
            'my-table',
            $csvFile,
        );
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyBranchInputBucketId,
            'my-table-2',
            $csvFile,
        );
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->emptyInputBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => $this->emptyInputBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = (new FakeDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        $expectedTableId = sprintf('%s.my-table', $this->emptyBranchInputBucketId);
        self::assertEquals($expectedTableId, $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => $expectedTableId,
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
                'column_types' => [
                    ['source' => 'id'],
                    ['source' => 'name'],
                ],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
        $expectedTableId = sprintf('%s.my-table-2', $this->emptyBranchInputBucketId);
        self::assertEquals($expectedTableId, $destinations->getTables()[1]->getSource());
        self::assertEquals('my-table-2.csv', $destinations->getTables()[1]->getDestination());
        self::assertEquals(
            [
                'source' => $expectedTableId,
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
                'column_types' => [
                    ['source' => 'foo'],
                    ['source' => 'bar'],
                ],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[1]->getDefinition(),
        );
        self::assertTrue($testHandler->hasInfoThatContains(
            sprintf('Using dev input "%s" instead of "%s.my-table-2".', $expectedTableId, $this->emptyInputBucketId),
        ));
    }

    #[NeedsEmptyInputBucket, NeedsDevBranch]
    public function testBranchRewriteTableStates(): void
    {
        $this->initEmptyFakeBranchInputBucket($this->initClient());
        $clientWrapper = $this->initClient($this->devBranchId);
        file_put_contents($this->temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($this->temp->getTmpFolder() . 'data.csv');
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyBranchInputBucketId,
            'my-table',
            $csvFile,
        );
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyBranchInputBucketId,
            'my-table-2',
            $csvFile,
        );
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $inputTablesStates = new InputTableStateList([
            [
                'source' => $this->emptyInputBucketId . '.my-table',
                'lastImportDate' => '1605741600',
            ],
            [
                'source' => $this->emptyInputBucketId . '.my-table-2',
                'lastImportDate' => '1605741600',
            ],
        ]);
        $destinations = (new FakeDevStorageTableRewriteHelper())->rewriteTableStatesDestinations(
            $inputTablesStates,
            $clientWrapper,
            $testLogger,
        );
        self::assertEquals(
            [
                [
                    'source' => sprintf('%s.my-table', $this->emptyBranchInputBucketId),
                    'lastImportDate' => '1605741600',
                ],
                [
                    'source' => sprintf('%s.my-table-2', $this->emptyBranchInputBucketId),
                    'lastImportDate' => '1605741600',
                ],
            ],
            $destinations->jsonSerialize(),
        );
        self::assertTrue($testHandler->hasInfoThatContains(
            sprintf(
                'Using dev input "%s.my-table-2" instead of "%s.my-table-2".',
                $this->emptyBranchInputBucketId,
                $this->emptyInputBucketId,
            ),
        ));
    }

    public function testIsDevelopmentBranchRewriteWithPrefix(): void
    {
        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->expects(self::once())->method('tableExists')
            ->with('out.c-123456-main.my-table')->willReturn(true);
        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::once())->method('getTable')
            ->willReturn(['id' => 'out.c-123456-main.my-table']);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($branchClientMock);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($storageClientMock);
        $clientWrapper->expects(self::once())->method('isDevelopmentBranch')->willReturn(true);
        $clientWrapper->method('getBranchId')->willReturn('123456');
        $clientWrapper->method('getDefaultBranch')->willReturn(
            new Branch('654321', 'main', true, null),
        );
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main.my-table',
                'destination' => 'my-table.csv',
            ],
        ]);
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $destinations = (new FakeDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertSame(
            [
                'source' => 'out.c-123456-main.my-table',
                'destination' => 'my-table.csv',
                'columns' => [],
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => (int) $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
    }

    /** @dataProvider provideBranchRewriteOptions */
    public function testIsDevelopmentBranchRewriteWithoutPrefix(
        string $sourceTable,
        string $destinationTable,
        int $checkCount,
        bool $isDevelopmentBranch,
    ): void {
        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->expects(self::exactly($checkCount))->method('tableExists')
            ->with($destinationTable)->willReturn(true);
        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::once())->method('getTable')
            ->willReturn(['id' => 'out.c-123456-main.my-table']);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($branchClientMock);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($storageClientMock);
        $clientWrapper->expects(self::once())->method('isDevelopmentBranch')->willReturn($isDevelopmentBranch);
        $clientWrapper->method('getBranchId')->willReturn('123456');
        $clientWrapper->method('getDefaultBranch')->willReturn(
            new Branch('654321', 'main', true, null),
        );
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $sourceTable,
                'destination' => 'my-table.csv',
            ],
        ]);
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $destinations = (new FakeDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertSame(
            [
                'source' => $destinationTable,
                'destination' => 'my-table.csv',
                'columns' => [],
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => (int) $clientWrapper->getDefaultBranch()->id,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
    }

    public function provideBranchRewriteOptions(): Generator
    {
        yield 'with prefix' => [
            'sourceTable' => 'out.c-main.my-table',
            'destinationTable' => 'out.c-123456-main.my-table',
            'checkCount' => 1,
            'hasBranch' => true,
        ];
        yield 'without prefix' => [
            'sourceTable' => 'out.main.my-table',
            'destinationTable' => 'out.123456-main.my-table',
            'checkCount' => 1,
            'hasBranch' => true,
        ];
        yield 'without prefix and without branch' => [
            'sourceTable' => 'out.main.my-table',
            'destinationTable' => 'out.main.my-table',
            'checkCount' => 0,
            'hasBranch' => false,
        ];
        yield 'with prefix and without branch' => [
            'sourceTable' => 'out.c-main.my-table',
            'destinationTable' => 'out.c-main.my-table',
            'checkCount' => 0,
            'hasBranch' => false,
        ];
    }
}
