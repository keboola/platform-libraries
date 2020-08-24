<?php

namespace Keboola\InputMapping\Tests\Strategy;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\InputMapping\Reader\Strategy\S3Strategy;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;

class S3StrategyTest extends \PHPUnit_Framework_TestCase
{
    /** @var Client */
    private $client;

    public function setUp()
    {
        parent::setUp();
        $this->client = new Client(['token' => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]);
        $tokenInfo = $this->client->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->client->getApiUrl()
        ));
        try {
            $this->client->dropBucket('in.c-input-mapping-test-strategy', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->client->createBucket('input-mapping-test-strategy', Client::STAGE_IN, 'Docker Testsuite');

        // Create table
        $temp = new Temp();
        $temp->initRunFolder();
        $csv = new CsvFile($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->client->createTableAsync('in.c-input-mapping-test-strategy', 'test1', $csv);
    }

    public function testColumns()
    {
        $strategy = new S3Strategy(
            $this->client,
            new NullLogger(),
            new NullWorkspaceProvider(),
            new InputTableStateList([]),
            '.'
        );
        $tableOptions = new InputTableOptions(
            [
                'source' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => 'some-table.csv',
                'columns' => ['Id', 'Name']
            ]
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertArrayHasKey('jobId', $result);
        self::assertArrayHasKey('table', $result);
        $job = $this->client->getJob($result['jobId']);
        self::assertEquals(
            'tableExport',
            $job['operationName']
        );
        self::assertEquals(
            ['Id', 'Name'],
            $job['operationParams']['export']['columns']
        );
    }

    public function testColumnsExtended()
    {
        $strategy = new S3Strategy(
            $this->client,
            new NullLogger(),
            new NullWorkspaceProvider(),
            new InputTableStateList([]),
            '.'
        );
        $tableOptions = new InputTableOptions(
            [
                'source' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => 'some-table.csv',
                'column_types' => [
                    [
                        'source' => 'Id',
                        'destination' => 'myid',
                        'type' => 'VARCHAR'
                    ],
                    [
                        'source' => 'Name',
                        'destination' => 'myname',
                        'type' => 'NUMERIC'
                    ],
                ],
            ]
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertArrayHasKey('jobId', $result);
        self::assertArrayHasKey('table', $result);
        $job = $this->client->getJob($result['jobId']);
        self::assertEquals(
            'tableExport',
            $job['operationName']
        );
        self::assertEquals(
            ['Id', 'Name'],
            $job['operationParams']['export']['columns']
        );
    }
}
