<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Helper\RealDevStorageTableRewriteHelper;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApiBranch\Branch;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Monolog\LogRecord;

class RealDevStorageTableRewriteHelperTest extends AbstractTestCase
{
    #[NeedsEmptyInputBucket, NeedsEmptyOutputBucket]
    public function testNoBranch(): void
    {
        $clientWrapper = $this->initClient();
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
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertEquals($this->emptyOutputBucketId . '.my-table', $destinations->getTables()[0]->getSource());
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

    #[NeedsEmptyInputBucket]
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
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertEquals($this->emptyInputBucketId . '.my-table', $destinations->getTables()[0]->getSource());
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
        self::assertTrue($testHandler->hasInfoThatContains(
            sprintf(
                'Using fallback to default branch "%s" for input "' . $this->emptyInputBucketId . '.my-table-2".',
                $clientWrapper->getDefaultBranch()->id,
            ),
        ));
    }

    #[NeedsEmptyInputBucket, NeedsDevBranch]
    public function testBranchRewriteTablesExists(): void
    {
        $this->initEmptyRealBranchInputBucket($this->initClient());

        $clientWrapper = $this->initClient($this->devBranchId);

        file_put_contents($this->temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($this->temp->getTmpFolder() . 'data.csv');

        $clientWrapper->getBranchClient()->createTableAsync(
            $this->emptyInputBucketId,
            'my-table',
            $csvFile,
        );
        $clientWrapper->getBranchClient()->createTableAsync(
            $this->emptyInputBucketId,
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
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        $expectedTableId = sprintf('%s.my-table', $this->emptyInputBucketId);
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
                'source_branch_id' => $this->devBranchId,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
        $expectedTableId = sprintf('%s.my-table-2', $this->emptyInputBucketId);
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
                'source_branch_id' => $this->devBranchId,
            ],
            $destinations->getTables()[1]->getDefinition(),
        );
        self::assertTrue($testHandler->hasInfoThatContains(
            sprintf(
                'Using dev input "%s.my-table-2" from branch "%s" instead of default branch "%s".',
                $this->emptyInputBucketId,
                $this->devBranchId,
                $clientWrapper->getDefaultBranch()->id,
            ),
        ));
    }

    #[NeedsEmptyInputBucket, NeedsDevBranch]
    public function testBranchRewriteTableStates(): void
    {
        $clientWrapper  = $this->initClient();
        $this->initEmptyRealBranchInputBucket($clientWrapper);

        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyInputBucketId,
            'my-table-2',
            $csv,
        );

        $clientWrapper = $this->initClient($this->devBranchId);

        $this->temp = new Temp('input-mapping');
        file_put_contents($this->temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($this->temp->getTmpFolder() . 'data.csv');
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyInputBucketId,
            'my-table',
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
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableStatesDestinations(
            $inputTablesStates,
            $clientWrapper,
            $testLogger,
        );
        // nothing is rewritten regardless of whether the bramch table exist or not
        self::assertEquals(
            [
                [
                    'source' => sprintf('%s.my-table', $this->emptyInputBucketId),
                    'lastImportDate' => '1605741600',
                ],
                [
                    'source' => sprintf('%s.my-table-2', $this->emptyInputBucketId),
                    'lastImportDate' => '1605741600',
                ],
            ],
            $destinations->jsonSerialize(),
        );
    }

    public function testIsDevelopmentBranchRewriteTableExists(): void
    {
        $storageClientMock = $this->createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::once())->method('tableExists')
            ->with('out.c-main.my-table')->willReturn(true);
        $storageClientMock->expects(self::once())->method('getTable')
            ->with('out.c-main.my-table')->willReturn(['name' => 'my-name']);
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($storageClientMock);
        $clientWrapper->method('isDevelopmentBranch')->willReturn(true);
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
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertSame(
            [
                'source' => 'out.c-main.my-table',
                'destination' => 'my-table.csv',
                'columns' => [],
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => 123456,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
        self::assertSame(['name' => 'my-name'], $destinations->getTables()[0]->getTableInfo());
    }

    /** @dataProvider provideBranchRewriteOptions */
    public function testIsDevelopmentBranchRewriteWithoutPrefix(
        string $sourceBranchId,
        int $checkCount,
        bool $branchTableExists,
        bool $isDevelopmentBranch,
        string $expectedName,
        int $expectedBranchCalls,
        int $expectedBasicCalls,
    ): void {
        $sourceTable = 'out.c-main.my-table';
        $storageClientMock = $this->createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::exactly($checkCount))->method('tableExists')
            ->with($sourceTable)->willReturn($branchTableExists);
        $storageClientMock->expects(self::exactly($expectedBranchCalls))->method('getTable')
            ->with($sourceTable)->willReturn(['name' => 'my-branch-name']);

        $defaultClientMock = $this->createMock(BranchAwareClient::class);
        $defaultClientMock->expects(self::exactly($expectedBasicCalls))->method('getTable')
            ->with($sourceTable)->willReturn(['name' => 'my-name']);
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($storageClientMock);
        $clientWrapper->method('getClientForDefaultBranch')->willReturn($defaultClientMock);
        $clientWrapper->method('isDevelopmentBranch')->willReturn($isDevelopmentBranch);
        $clientWrapper->method('getDefaultBranch')->willReturn(
            new Branch('654321', 'main', true, null),
        );
        $clientWrapper->method('getBranchId')->willReturn('123456');
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $sourceTable,
                'destination' => 'my-table.csv',
            ],
        ]);
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger,
        );
        self::assertSame(
            [
                'source' => $sourceTable,
                'destination' => 'my-table.csv',
                'columns' => [],
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
                'use_view' => false,
                'keep_internal_timestamp_column' => true,
                'source_branch_id' => (int) $sourceBranchId,
            ],
            $destinations->getTables()[0]->getDefinition(),
        );
        self::assertSame(
            ['name' => $expectedName],
            $destinations->getTables()[0]->getTableInfo(),
        );
    }

    public function provideBranchRewriteOptions(): Generator
    {
        yield 'exists with branch' => [
            'source_branch_id' => '123456',
            'checkCount' => 1,
            'branchTableExists' => true,
            'hasBranch' => true,
            'expectedName' => 'my-branch-name',
            'expectedBranchCalls' => 1,
            'expectedBasicCalls' => 0,
        ];
        yield 'does not exist with branch' => [
            'source_branch_id' => '654321',
            'checkCount' => 1,
            'branchTableExists' => false,
            'hasBranch' => true,
            'expectedName' => 'my-name',
            'expectedBranchCalls' => 0,
            'expectedBasicCalls' => 1,
        ];
        yield 'exists without branch' => [
            'source_branch_id' => '654321',
            'checkCount' => 0,
            'branchTableExists' => true,
            'hasBranch' => false,
            'expectedName' => 'my-name',
            'expectedBranchCalls' => 0,
            'expectedBasicCalls' => 1,
        ];
        yield 'does not exist without branch' => [
            'source_branch_id' => '654321',
            'checkCount' => 0,
            'branchTableExists' => false,
            'hasBranch' => false,
            'expectedName' => 'my-name',
            'expectedBranchCalls' => 0,
            'expectedBasicCalls' => 1,
        ];
    }

    public function testUseBranchFromMappingDefinition(): void
    {
        $mainBranch = new Branch('123', 'main', true);
        $devBranch1 =  new Branch('456', 'dev-1', false);
        $devBranch2 =  new Branch('789', 'dev-2', false);

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->expects(self::exactly(2))
            ->method('getBranchId')
            ->willReturn($devBranch2->id)
        ;

        $clientWrapperMock->expects(self::exactly(2))
            ->method('isDevelopmentBranch')
            ->willReturn(true)
        ;

        $clientWrapperMock->expects(self::exactly(3))
            ->method('getDefaultBranch')
            ->willReturn($mainBranch)
        ;

        $defaultBranchClientMock = $this->createMock(BranchAwareClient::class);
        $defaultBranchClientMock->expects(self::once())
            ->method('getTable')
            ->with('in.c-myBucket.my-table-3')
            ->willReturn([
                'id' => 'in.c-myBucket.my-table-3',
                'name' => 'my-table-3',
            ])
        ;

        $clientWrapperMock->expects(self::once())
            ->method('getClientForDefaultBranch')
            ->willReturn($defaultBranchClientMock)
        ;

        $branch1ClientMock = $this->createMock(BranchAwareClient::class);
        $branch1ClientMock->expects(self::once())
            ->method('getTable')
            ->with('in.c-myBucket.my-table')
            ->willReturn([
                'id' => 'in.c-myBucket.my-table',
                'name' => 'my-table',
            ]);

        $clientWrapperMock->expects(self::once())
            ->method('getClientForBranch')
            ->with('456')
            ->willReturn($branch1ClientMock)
        ;

        $branch2ClientMock = $this->createMock(BranchAwareClient::class);
        $tableExistsExpectedParams = [
            'in.c-myBucket.my-table-2',
            'in.c-myBucket.my-table-3',
        ];

        $tableExistsReturnValues = [
            true,
            false,
        ];

        $branch2ClientMock->expects(self::exactly(2))
            ->method('tableExists')
            ->willReturnCallback(
                function (string $tableId) use (&$tableExistsExpectedParams, &$tableExistsReturnValues): bool {
                    self::assertSame(array_shift($tableExistsExpectedParams), $tableId);
                    return (bool) array_shift($tableExistsReturnValues);
                },
            )
        ;

        $branch2ClientMock->expects(self::once())
            ->method('getTable')
            ->with('in.c-myBucket.my-table-2')
            ->willReturn([
                'id' => 'in.c-myBucket.my-table-2',
                'name' => 'my-table-2',
            ])
        ;

        $clientWrapperMock->expects(self::exactly(3))
            ->method('getBranchClient')
            ->willReturn($branch2ClientMock)
        ;

        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'in.c-myBucket.my-table',
                'source_branch_id' => $devBranch1->id,
                'destination' => 'my-table.csv',
            ],
            [
                'source' => 'in.c-myBucket.my-table-2', // from branch 2
                'destination' => 'my-table-2.csv',
            ],
            [
                'source' => 'in.c-myBucket.my-table-3', // from default branch
                'destination' => 'my-table-3.csv',
            ],
        ]);
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapperMock,
            $testLogger,
        );

        $inputOptions = $destinations->getTables();
        $records = $testHandler->getRecords();
        self::assertCount(3, $inputOptions);
        self::assertCount(3, $records);

        self::assertSame('in.c-myBucket.my-table', $inputOptions[0]->getSource());
        self::assertSame(456, $inputOptions[0]->getSourceBranchId());
        /** @var LogRecord $record */
        $record = array_shift($records);
        self::assertSame('INFO', $record->level->getName());
        self::assertSame(
            'Using input "in.c-myBucket.my-table" from dev branch "456".',
            $record->message,
        );

        self::assertSame('in.c-myBucket.my-table-2', $inputOptions[1]->getSource());
        self::assertSame(789, $inputOptions[1]->getSourceBranchId());
        /** @var LogRecord $record */
        $record = array_shift($records);
        self::assertSame('INFO', $record->level->getName());
        self::assertSame(
            'Using dev input "in.c-myBucket.my-table-2" from branch "789" instead of default branch "123".',
            $record->message,
        );

        self::assertSame('in.c-myBucket.my-table-3', $inputOptions[2]->getSource());
        self::assertSame(123, $inputOptions[2]->getSourceBranchId());
        /** @var LogRecord $record */
        $record = array_shift($records);
        self::assertSame('INFO', $record->level->getName());
        self::assertSame(
            'Using fallback to default branch "123" for input "in.c-myBucket.my-table-3".',
            $record->message,
        );
    }
}
