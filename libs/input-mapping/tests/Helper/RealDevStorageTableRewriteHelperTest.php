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
        $clientWrapper = $this->getClientWrapper($this->branchId);
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
        // nothing is rewriteen regardless of whether the table exist or not
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

    public function testHasBranchRewriteTableExists(): void
    {
        $storageClientMock = self::createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::once())   ->method('tableExists')
            ->with('out.c-main.my-table')->willReturn(true);
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($storageClientMock);
        $clientWrapper->method('hasBranch')->willReturn(true);
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
                'sourceBranchId' => '123456',
            ],
            $destinations->getTables()[0]->getDefinition()
        );
    }

    /** @dataProvider provideBranchRewriteOptions */
    public function testHasBranchRewriteWithoutPrefix(
        string $sourceBranchId,
        int $checkCount,
        bool $branchTableExists,
        bool $hasBranch,
    ): void {
        $sourceTable = 'out.c-main.my-table';
        $storageClientMock = self::createMock(BranchAwareClient::class);
        $storageClientMock->expects(self::exactly($checkCount))->method('tableExists')
            ->with($sourceTable)->willReturn($branchTableExists);
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($storageClientMock);
        $clientWrapper->method('hasBranch')->willReturn($hasBranch);
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
                'sourceBranchId' => $sourceBranchId,
            ],
            $destinations->getTables()[0]->getDefinition()
        );
    }

    public function provideBranchRewriteOptions(): Generator
    {
        yield 'exists with branch' => [
            'sourceBranchId' => '123456',
            'checkCount' => 1,
            'branchTableExists' => true,
            'hasBranch' => true,
        ];
        yield 'does not exist with branch' => [
            'sourceBranchId' => '654321',
            'checkCount' => 1,
            'branchTableExists' => false,
            'hasBranch' => true,
        ];
        yield 'exists without branch' => [
            'sourceBranchId' => '654321',
            'checkCount' => 0,
            'branchTableExists' => true,
            'hasBranch' => false,
        ];
        yield 'does not exist without branch' => [
            'sourceBranchId' => '654321',
            'checkCount' => 0,
            'branchTableExists' => false,
            'hasBranch' => false,
        ];
    }
}
