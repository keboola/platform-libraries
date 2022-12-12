<?php

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Helper\SourceRewriteHelper;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class SourceRewriteHelperTest extends TestCase
{
    private string $branchId;

    public function setUp()
    {
        parent::setUp();
        $clientWrapper = $this->getClientWrapper(null);
        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'dev branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $this->branchId = $branches->createBranch('dev branch')['id'];
    }

    protected function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId),
        );
    }

    private function initBuckets()
    {
        $clientWrapper = $this->getClientWrapper(null);
        try {
            $clientWrapper->getBasicClient()->dropBucket('out.c-main', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        try {
            $clientWrapper->getBasicClient()->dropBucket('out.c-dev-branch-main', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        foreach ($clientWrapper->getBasicClient()->listBuckets() as $bucket) {
            if (preg_match('/^c\-[0-9]+\-output\-mapping\-test$/ui', $bucket['name'])) {
                $clientWrapper->getBasicClient()->dropBucket($bucket['id'], ['force' => true]);
            }
        }

        $clientWrapper->getBasicClient()->createBucket('main', 'out');
        $clientWrapper->getBasicClient()->createBucket($this->branchId . '-main', 'out');
    }

    public function testNoBranch()
    {
        $clientWrapper = $this->getClientWrapper(null);
        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main.my-table',
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
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources($inputTablesOptions, $clientWrapper, $testLogger);
        self::assertEquals('out.c-main.my-table', $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => 'out.c-main.my-table',
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
                'format' => 'rfc',
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
                'format' => 'rfc',
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }

    public function testInvalidName()
    {
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ]
        ]);
        self::expectException(InputOperationException::class);
        self::expectExceptionMessage('Invalid destination: "out.c-main"');
        SourceRewriteHelper::rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
        );
    }

    public function testBranchRewriteNoTables()
    {
        $this->initBuckets();
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => 'out.c-main.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
        );
        self::assertEquals('out.c-main.my-table', $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => 'out.c-main.my-table',
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
                'format' => 'rfc',
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        self::assertEquals('out.c-main.my-table-2', $destinations->getTables()[1]->getSource());
        self::assertEquals('my-table-2.csv', $destinations->getTables()[1]->getDestination());
        self::assertEquals(
            [
                'source' => 'out.c-main.my-table-2',
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
                'format' => 'rfc',
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }

    public function testBranchRewriteTablesExists()
    {
        $this->initBuckets();
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $temp = new Temp(uniqid('input-mapping'));
        $temp->initRunFolder();
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');

        $branchBucketId = sprintf('out.c-%s-main', $this->branchId);
        $clientWrapper->getBasicClient()->createTable($branchBucketId, 'my-table', $csvFile);
        $clientWrapper->getBasicClient()->createTable($branchBucketId, 'my-table-2', $csvFile);
        $testLogger = new TestLogger();
        $inputTablesOptions = new InputTableOptionsList([
            [
                'source' => 'out.c-main.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
            ],
            [
                'source' => 'out.c-main.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
            ],
        ]);
        $destinations = SourceRewriteHelper::rewriteTableOptionsSources(
            $inputTablesOptions,
            $clientWrapper,
            $testLogger
        );
        $expectedTableId = sprintf('%s.my-table', $branchBucketId);
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
                'format' => 'rfc',
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        $expectedTableId = sprintf('%s.my-table-2', $branchBucketId);
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
                'format' => 'rfc',
            ],
            $destinations->getTables()[1]->getDefinition()
        );
        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev input "%s" instead of "out.c-main.my-table-2".', $expectedTableId)
        ));
    }

    public function testBranchRewriteTableStates()
    {
        $this->initBuckets();
        $clientWrapper = $this->getClientWrapper($this->branchId);
        $temp = new Temp(uniqid('input-mapping'));
        $temp->initRunFolder();
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');
        $branchBucketId = sprintf('out.c-%s-main', $this->branchId);
        $clientWrapper->getBasicClient()->createTable($branchBucketId, 'my-table', $csvFile);
        $clientWrapper->getBasicClient()->createTable($branchBucketId, 'my-table-2', $csvFile);
        $testLogger = new TestLogger();
        $inputTablesStates = new InputTableStateList([
            [
                'source' => 'out.c-main.my-table',
                'lastImportDate' => '1605741600',
            ],
            [
                'source' => 'out.c-main.my-table-2',
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
                    'source' => sprintf('%s.my-table', $branchBucketId),
                    'lastImportDate' => '1605741600',
                ],
                [
                    'source' => sprintf('%s.my-table-2', $branchBucketId),
                    'lastImportDate' => '1605741600',
                ],
            ],
            $destinations->jsonSerialize()
        );
        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev input "%s.my-table-2" instead of "out.c-main.my-table-2".', $branchBucketId)
        ));
    }
}
