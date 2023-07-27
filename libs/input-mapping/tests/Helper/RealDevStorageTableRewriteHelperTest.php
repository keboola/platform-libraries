<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Helper\RealDevStorageTableRewriteHelper;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class RealDevStorageTableRewriteHelperTest extends TestCase
{
    private string $branchId;
    private string $outBucketId;

    public function setUp(): void
    {
        parent::setUp();
        $clientWrapper = $this->getClientWrapper(null);
        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === self::class) {
                $branches->deleteBranch($branch['id']);
            }
        }
        $this->branchId = (string) $branches->createBranch(self::class)['id'];
    }

    protected function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId
            ),
        );
    }

    private function initBuckets(): void
    {
        $clientWrapper = $this->getClientWrapper(null);

        $inBucketId = TestSatisfyer::getBucketIdByDisplayName($clientWrapper, 'main', Client::STAGE_IN);
        if ($inBucketId) {
            $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                (string) $inBucketId,
                ['force' => true, 'async' => true]
            );
        }

        $outBucketId = TestSatisfyer::getBucketIdByDisplayName($clientWrapper, 'main', Client::STAGE_OUT);
        if ($outBucketId) {
            $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                (string) $outBucketId,
                ['force' => true, 'async' => true]
            );
        }

        $outDevBucketId = TestSatisfyer::getBucketIdByDisplayName(
            $clientWrapper,
            'dev-branch-main',
            Client::STAGE_OUT
        );
        if ($outDevBucketId) {
            $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                (string) $outDevBucketId,
                ['force' => true, 'async' => true]
            );
        }

        foreach ($clientWrapper->getTableAndFileStorageClient()->listBuckets() as $bucket) {
            if (preg_match('/^(c-)?[0-9]+-output-mapping-test$/ui', $bucket['name'])) {
                $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                    $bucket['id'],
                    ['force' => true, 'async' => true]
                );
            }
        }

        $this->outBucketId = $clientWrapper->getBasicClient()->createBucket('main', Client::STAGE_OUT);

        $clientWrapper = $this->getClientWrapper($this->branchId);
        $clientWrapper->getBranchClient()->createBucket(
            'main',
            Client::STAGE_OUT
        );
    }

    public function testNoBranch(): void
    {
        $this->initBuckets();
        $clientWrapper = $this->getClientWrapper(null);
        $this->outBucketId = $clientWrapper->getBasicClient()->createBucket('main', Client::STAGE_IN);
        $temp = new Temp();
        $csv = new CsvFile($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        // Create table
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->outBucketId,
            'my-table',
            $csv
        );
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            'in.c-main',
            'my-table-2',
            $csv
        );

        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->outBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => 'in.c-main.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
        );
        self::assertEquals($this->outBucketId . '.my-table', $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => $this->outBucketId . '.my-table',
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
                'sourceBranchId' => $clientWrapper->getDefaultBranch()['branchId'],
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        self::assertEquals('in.c-main.my-table-2', $destinations->getTables()[1]->getSource());
        self::assertEquals('my-table-2.csv', $destinations->getTables()[1]->getDestination());
        self::assertEquals(
            [
                'source' => 'in.c-main.my-table-2',
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
                'sourceBranchId' => $clientWrapper->getDefaultBranch()['branchId'],
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }


    public function testBranchRewriteNoTables(): void
    {
        $this->initBuckets();
        $temp = new Temp();
        $csv = new CsvFile($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        $clientWrapper = $this->getClientWrapper(null);
        // Create table
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->outBucketId,
            'my-table',
            $csv
        );
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->outBucketId,
            'my-table-2',
            $csv
        );

        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->outBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => $this->outBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
        );
        self::assertEquals($this->outBucketId . '.my-table', $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => $this->outBucketId . '.my-table',
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
                'sourceBranchId' => $clientWrapper->getDefaultBranch()['branchId'],
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        self::assertEquals($this->outBucketId . '.my-table-2', $destinations->getTables()[1]->getSource());
        self::assertEquals('my-table-2.csv', $destinations->getTables()[1]->getDestination());
        self::assertEquals(
            [
                'source' => $this->outBucketId . '.my-table-2',
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
                'sourceBranchId' => $clientWrapper->getDefaultBranch()['branchId'],
            ],
            $destinations->getTables()[1]->getDefinition()
        );
        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf(
                'Using fallback to default branch "%s" for input "out.c-main.my-table-2".',
                $clientWrapper->getDefaultBranch()['branchId']
            )
        ));
    }

    public function testBranchRewriteTablesExists(): void
    {
        $this->initBuckets();
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $temp = new Temp(uniqid('input-mapping'));
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');

        $clientWrapper->getBranchClient()->createTableAsync(
            $this->outBucketId,
            'my-table',
            $csvFile
        );
        $clientWrapper->getBranchClient()->createTableAsync(
            $this->outBucketId,
            'my-table-2',
            $csvFile
        );
        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $this->outBucketId . '.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => $this->outBucketId . '.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
        );
        $expectedTableId = sprintf('%s.my-table', $this->outBucketId);
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
                'sourceBranchId' => $this->branchId,
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        $expectedTableId = sprintf('%s.my-table-2', $this->outBucketId);
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
                'sourceBranchId' => $this->branchId,
            ],
            $destinations->getTables()[1]->getDefinition()
        );
        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf(
                'Using dev input "out.c-main.my-table-2" from branch "%s" instead of main branch "%s".',
                $this->branchId,
                $clientWrapper->getDefaultBranch()['branchId'],
            )
        ));
    }

    public function testBranchRewriteTableStates(): void
    {
        $this->initBuckets();
        $temp = new Temp();
        $csv = new CsvFile($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $clientWrapper = $this->getClientWrapper(null);
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->outBucketId,
            'my-table-2',
            $csv
        );

        $clientWrapper = $this->getClientWrapper($this->branchId);
        $temp = new Temp(uniqid('input-mapping'));
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->outBucketId,
            'my-table',
            $csvFile
        );
        $testLogger = new TestLogger();
        $inputTablesStates = new InputTableStateList([
            [
                'source' => $this->outBucketId . '.my-table',
                'lastImportDate' => '1605741600',
            ],
            [
                'source' => $this->outBucketId . '.my-table-2',
                'lastImportDate' => '1605741600',
            ],
        ]);
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableStatesDestinations(
            $inputTablesStates,
            $clientWrapper,
            $testLogger
        );
        // nothing is rewritten regardless of whether the bramch table exist or not
        self::assertEquals(
            [
                [
                    'source' => sprintf('%s.my-table', $this->outBucketId),
                    'lastImportDate' => '1605741600',
                ],
                [
                    'source' => sprintf('%s.my-table-2', $this->outBucketId),
                    'lastImportDate' => '1605741600',
                ],
            ],
            $destinations->jsonSerialize()
        );
    }

    public function testIsDevelopmentBranchRewriteTableExists(): void
    {
        $storageClientMock = self::createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::once())->method('tableExists')
            ->with('out.c-main.my-table')->willReturn(true);
        $storageClientMock->expects(self::once())->method('getTable')
            ->with('out.c-main.my-table')->willReturn(['name' => 'my-name']);
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($storageClientMock);
        $clientWrapper->method('isDevelopmentBranch')->willReturn(true);
        $clientWrapper->method('getBranchId')->willReturn('123456');
        $clientWrapper->method('getDefaultBranch')->willReturn(['branchId' => '654321']);

        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main.my-table',
                'destination' => 'my-table.csv',
            ],
        ]);
        $testLogger = new TestLogger();
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
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
                'sourceBranchId' => 123456,
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        self::assertSame(['name' => 'my-name'], $destinations->getTables()[0]->getTableInfo());
    }

    /** @dataProvider provideBranchRewriteOptions */
    public function testIsDevelopmentBranchRewriteWithoutPrefix(
        string $sourceBranchId,
        int    $checkCount,
        bool   $branchTableExists,
        bool   $isDevelopmentBranch,
        string $expectedName,
        int    $expectedBranchCalls,
        int    $expectedBasicCalls,
    ): void {
        $sourceTable = 'out.c-main.my-table';
        $storageClientMock = self::createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::exactly($checkCount))->method('tableExists')
            ->with($sourceTable)->willReturn($branchTableExists);
        $storageClientMock->expects(self::exactly($expectedBranchCalls))->method('getTable')
            ->with($sourceTable)->willReturn(['name' => 'my-branch-name']);

        $basicStorageClientMock = self::createMock(Client::class);
        $basicStorageClientMock->expects(self::exactly($expectedBasicCalls))->method('getTable')
            ->with($sourceTable)->willReturn(['name' => 'my-name']);
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($storageClientMock);
        $clientWrapper->method('getBasicClient')->willReturn($basicStorageClientMock);
        $clientWrapper->method('isDevelopmentBranch')->willReturn($isDevelopmentBranch);
        $clientWrapper->method('getDefaultBranch')->willReturn(['branchId' => '654321']);
        $clientWrapper->method('getBranchId')->willReturn('123456');
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $sourceTable,
                'destination' => 'my-table.csv',
            ],
        ]);
        $testLogger = new TestLogger();
        $destinations = (new RealDevStorageTableRewriteHelper())->rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
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
                'sourceBranchId' => (int) $sourceBranchId,
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        self::assertSame(
            ['name' => $expectedName],
            $destinations->getTables()[0]->getTableInfo()
        );
    }

    public function provideBranchRewriteOptions(): Generator
    {
        yield 'exists with branch' => [
            'sourceBranchId' => '123456',
            'checkCount' => 1,
            'branchTableExists' => true,
            'hasBranch' => true,
            'expectedName' => 'my-branch-name',
            'expectedBranchCalls' => 1,
            'expectedBasicCalls' => 0,
        ];
        yield 'does not exist with branch' => [
            'sourceBranchId' => '654321',
            'checkCount' => 1,
            'branchTableExists' => false,
            'hasBranch' => true,
            'expectedName' => 'my-name',
            'expectedBranchCalls' => 0,
            'expectedBasicCalls' => 1,
        ];
        yield 'exists without branch' => [
            'sourceBranchId' => '654321',
            'checkCount' => 0,
            'branchTableExists' => true,
            'hasBranch' => false,
            'expectedName' => 'my-name',
            'expectedBranchCalls' => 0,
            'expectedBasicCalls' => 1,
        ];
        yield 'does not exist without branch' => [
            'sourceBranchId' => '654321',
            'checkCount' => 0,
            'branchTableExists' => false,
            'hasBranch' => false,
            'expectedName' => 'my-name',
            'expectedBranchCalls' => 0,
            'expectedBasicCalls' => 1,
        ];
    }
}
