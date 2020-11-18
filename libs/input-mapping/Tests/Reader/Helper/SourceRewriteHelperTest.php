<?php

namespace Reader\Helper;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Reader\Helper\SourceRewriteHelper;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class SourceRewriteHelperTest extends TestCase
{
    /** @var ClientWrapper */
    private $clientWrapper;

    public function setUp()
    {
        parent::setUp();
        $this->clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
            null,
            null
        );
    }

    private function initBuckets()
    {
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('out.c-main', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('out.c-dev-branch-main', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket('main', 'out');
        $this->clientWrapper->getBasicClient()->createBucket('dev-branch-main', 'out');
    }

    public function testNoBranch()
    {
        $this->clientWrapper->setBranch('');
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
        $destinations = SourceRewriteHelper::rewriteDestinations($inputTablesOptions, $this->clientWrapper, $testLogger);
        self::assertEquals('out.c-main.my-table', $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => 'out.c-main.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
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
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }

    public function testInvalidName()
    {
        $this->clientWrapper->setBranch('test');
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
        SourceRewriteHelper::rewriteDestinations(
            $inputTablesOptions,
            $this->clientWrapper,
            $testLogger
        );
    }

    public function testBranchRewriteNoTables()
    {
        $this->initBuckets();
        $this->clientWrapper->setBranch('dev-branch');
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
        $destinations = SourceRewriteHelper::rewriteDestinations(
            $inputTablesOptions,
            $this->clientWrapper,
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
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
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
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }

    public function testBranchRewriteTablesExists()
    {
        $this->initBuckets();
        $this->clientWrapper->setBranch('dev-branch');
        $temp = new Temp(uniqid('input-mapping'));
        $temp->initRunFolder();
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');
        $this->clientWrapper->getBasicClient()->createTable('out.c-dev-branch-main', 'my-table', $csvFile);
        $this->clientWrapper->getBasicClient()->createTable('out.c-dev-branch-main', 'my-table-2', $csvFile);
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
        $destinations = SourceRewriteHelper::rewriteDestinations(
            $inputTablesOptions,
            $this->clientWrapper,
            $testLogger
        );
        self::assertEquals('out.c-dev-branch-main.my-table', $destinations->getTables()[0]->getSource());
        self::assertEquals('my-table.csv', $destinations->getTables()[0]->getDestination());
        self::assertEquals(
            [
                'source' => 'out.c-dev-branch-main.my-table',
                'destination' => 'my-table.csv',
                'days' => 12,
                'columns' => ['id', 'name'],
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
            ],
            $destinations->getTables()[0]->getDefinition()
        );
        self::assertEquals('out.c-dev-branch-main.my-table-2', $destinations->getTables()[1]->getSource());
        self::assertEquals('my-table-2.csv', $destinations->getTables()[1]->getDestination());
        self::assertEquals(
            [
                'source' => 'out.c-dev-branch-main.my-table-2',
                'destination' => 'my-table-2.csv',
                'columns' => ['foo', 'bar'],
                'column_types' => [],
                'where_values' => [],
                'where_operator' => 'eq',
                'overwrite' => false,
            ],
            $destinations->getTables()[1]->getDefinition()
        );
    }
}
