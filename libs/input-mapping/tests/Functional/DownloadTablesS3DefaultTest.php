<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;

class DownloadTablesS3DefaultTest extends DownloadTablesTestAbstract
{
    public function setUp(): void
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket('docker-test', Client::STAGE_IN, 'Docker Testsuite');

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-docker-test', 'test', $csv);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-docker-test', 'test2', $csv);
    }

    public function testReadTablesS3DefaultBackend(): void
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv',
            ],
            [
                'source' => 'in.c-docker-test.test2',
                'destination' => 'test2.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::S3,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals('in.c-docker-test.test', $manifest['id']);
        $this->assertS3info($manifest);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.csv.manifest');
        self::assertEquals('in.c-docker-test.test2', $manifest['id']);
        $this->assertS3info($manifest);
        self::assertTrue($logger->hasInfoThatContains('Processing 2 S3 table exports.'));
    }

    public function testReadTablesABSUnsupportedBackend(): void
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv',
            ],
            [
                'source' => 'in.c-docker-test.test2',
                'destination' => 'test2.csv',
            ],
        ]);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('This project does not have ABS backend.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::ABS,
            new ReaderOptions(true)
        );
    }
}
