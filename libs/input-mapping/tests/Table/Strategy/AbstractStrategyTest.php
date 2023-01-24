<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

abstract class AbstractStrategyTest extends TestCase
{
    protected ClientWrapper $clientWrapper;
    protected Temp $temp;
    public function setUp(): void
    {
        parent::setUp();
        $this->temp = new Temp();
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
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
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-input-mapping-test-strategy', 'test1', $csv);
    }
}
