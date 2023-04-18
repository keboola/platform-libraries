<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Helper\SourceRewriteHelper;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class SourceRewriteHelperTest extends TestCase
{
    private string $branchId;
    private string $outBucketId;
    private string $outBranchBucketId;

    public function setUp(): void
    {
        parent::setUp();
        $clientWrapper = $this->getClientWrapper(null);
        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'dev branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $this->branchId = (string) $branches->createBranch('dev branch')['id'];
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
            $clientWrapper->getBasicClient()->dropBucket((string) $outBucketId, ['force' => true, 'async' => true]);
        }

        $outDevBucketId = TestSatisfyer::getBucketIdByDisplayName(
            $clientWrapper,
            'dev-branch-main',
            Client::STAGE_OUT
        );
        if ($outDevBucketId) {
            $clientWrapper->getBasicClient()->dropBucket((string) $outDevBucketId, ['force' => true, 'async' => true]);
        }

        foreach ($clientWrapper->getBasicClient()->listBuckets() as $bucket) {
            if (preg_match('/^(c-)?[0-9]+-output-mapping-test$/ui', $bucket['name'])) {
                $clientWrapper->getBasicClient()->dropBucket($bucket['id'], ['force' => true, 'async' => true]);
            }
        }

        $this->outBucketId = $clientWrapper->getBasicClient()->createBucket('main', Client::STAGE_OUT);
        $this->outBranchBucketId = $clientWrapper->getBasicClient()->createBucket(
            $this->branchId . '-main',
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
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources(
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
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }

    public function testInvalidName(): void
    {
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
        ]);
        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('Invalid destination: "out.c-main"');
        SourceRewriteHelper::rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
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
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources(
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
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }

    public function testBranchRewriteTablesExists(): void
    {
        $this->initBuckets();
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $temp = new Temp(uniqid('input-mapping'));
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');

        $clientWrapper->getBasicClient()->createTableAsync($this->outBranchBucketId, 'my-table', $csvFile);
        $clientWrapper->getBasicClient()->createTableAsync($this->outBranchBucketId, 'my-table-2', $csvFile);
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
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
        );
        $expectedTableId = sprintf('%s.my-table', $this->outBranchBucketId);
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
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        $expectedTableId = sprintf('%s.my-table-2', $this->outBranchBucketId);
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
            ],
            $destinations->getTables()[1]->getDefinition()
        );
        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev input "%s" instead of "%s.my-table-2".', $expectedTableId, $this->outBucketId)
        ));
    }

    public function testBranchRewriteTableStates(): void
    {
        $this->initBuckets();
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $temp = new Temp(uniqid('input-mapping'));
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');
        $clientWrapper->getBasicClient()->createTableAsync($this->outBranchBucketId, 'my-table', $csvFile);
        $clientWrapper->getBasicClient()->createTableAsync($this->outBranchBucketId, 'my-table-2', $csvFile);
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
        $destinations = SourceRewriteHelper::rewriteTableStatesDestinations(
            $inputTablesStates,
            $clientWrapper,
            $testLogger
        );
        self::assertEquals(
            [
                [
                    'source' => sprintf('%s.my-table', $this->outBranchBucketId),
                    'lastImportDate' => '1605741600',
                ],
                [
                    'source' => sprintf('%s.my-table-2', $this->outBranchBucketId),
                    'lastImportDate' => '1605741600',
                ],
            ],
            $destinations->jsonSerialize()
        );
        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf(
                'Using dev input "%s.my-table-2" instead of "%s.my-table-2".',
                $this->outBranchBucketId,
                $this->outBucketId
            )
        ));
    }

    public function testHasBranchRewriteWithPrefix(): void
    {
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('tableExists')
            ->with('out.c-123456-main.my-table')->willReturn(true);
        $clientMock->expects(self::once())->method('webalizeDisplayName')->willReturnCallback(
            fn ($argument) => ['displayName' => $argument]
        );
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($clientMock);
        $clientWrapper->expects(self::once())->method('hasBranch')->willReturn(true);
        $clientWrapper->method('getBranchId')->willReturn('123456');
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main.my-table',
                'destination' => 'my-table.csv',
            ],
        ]);
        $testLogger = new TestLogger();
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
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
            ],
            $destinations->getTables()[0]->getDefinition()
        );
    }

    /** @dataProvider provideBranchRewriteOptions */
    public function testHasBranchRewriteWithoutPrefix(
        string $sourceTable,
        string $destinationTable,
        int $checkCount,
        bool $hasBranch,
    ): void {
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly($checkCount))->method('tableExists')
            ->with($destinationTable)->willReturn(true);
        $clientMock->expects(self::exactly($checkCount))->method('webalizeDisplayName')->willReturnCallback(
            fn ($argument) => ['displayName' => $argument]
        );
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($clientMock);
        $clientWrapper->expects(self::once())->method('hasBranch')->willReturn($hasBranch);
        $clientWrapper->method('getBranchId')->willReturn('123456');
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => $sourceTable,
                'destination' => 'my-table.csv',
            ],
        ]);
        $testLogger = new TestLogger();
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
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
            ],
            $destinations->getTables()[0]->getDefinition()
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
