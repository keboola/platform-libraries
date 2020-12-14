<?php

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Strategy\S3;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class S3StrategyTest extends TestCase
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
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBasicClient()->getApiUrl()
        ));
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-input-mapping-test-strategy', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket(
            'input-mapping-test-strategy',
            Client::STAGE_IN,
            'Docker Testsuite'
        );

        // Create table
        $temp = new Temp();
        $temp->initRunFolder();
        $csv = new CsvFile($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-input-mapping-test-strategy', 'test1', $csv);
    }

    public function testColumns()
    {
        $strategy = new S3(
            $this->clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
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
        $job = $this->clientWrapper->getBasicClient()->getJob($result['jobId']);
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
        $strategy = new S3(
            $this->clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
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
        $job = $this->clientWrapper->getBasicClient()->getJob($result['jobId']);
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
