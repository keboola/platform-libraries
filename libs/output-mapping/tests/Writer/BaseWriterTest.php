<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseWriterTest extends \PHPUnit_Framework_TestCase
{
    protected ClientWrapper $clientWrapper;
    protected Temp $tmp;

    protected function clearBuckets($buckets)
    {
        foreach ($buckets as $bucket) {
            try {
                $this->clientWrapper->getBasicClient()->dropBucket($bucket, ['force' => true]);
            } catch (ClientException $e) {
                if ($e->getCode() != 404) {
                    throw $e;
                }
            }
        }
    }

    protected function clearFileUploads($tags)
    {
        // Delete file uploads
        $options = new ListFilesOptions();
        $options->setTags($tags);
        sleep(1);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
        }
    }

    public function setUp()
    {
        parent::setUp();
        $this->tmp = new Temp();
        $this->tmp->initRunFolder();
        $root = $this->tmp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'download');
        $this->initClient();
    }

    protected function initClient(?string $branchId = null)
    {
        $clientOptions = (new ClientOptions())
            ->setUrl(STORAGE_API_URL)
            ->setToken(STORAGE_API_TOKEN)
            ->setBranchId($branchId)
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            });
        $this->clientWrapper = new ClientWrapper($clientOptions);
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBasicClient()->getApiUrl()
        ));
    }

    protected function getStagingFactory($clientWrapper = null, $format = 'json', $logger = null)
    {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ? $clientWrapper : $this->clientWrapper,
            $logger ? $logger : new NullLogger(),
            $format
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->tmp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                StrategyFactory::LOCAL => new Scope([
                    Scope::TABLE_DATA, Scope::TABLE_METADATA,
                    Scope::FILE_DATA, Scope::FILE_METADATA
                ])
            ]
        );
        return $stagingFactory;
    }

    protected function assertTablesExists(string $bucketId, array $expectedTables)
    {
        $tables = $this->clientWrapper->getBasicClient()->listTables($bucketId);
        $tableIds = array_column($tables, 'id');

        // order of listed tables is not guaranteed
        sort($tableIds);
        sort($expectedTables);

        $this->assertSame($expectedTables, $tableIds);
    }

    protected function assertTableRowsEquals($tableName, array $expectedRows)
    {
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview($tableName);

        $rows = explode("\n", trim($data));
        // convert to lowercase because of https://keboola.atlassian.net/browse/KBC-864
        $rows = array_map('strtolower', $rows);

        // order of rows is not guaranteed
        $rows = sort($rows);
        $expectedRows = sort($expectedRows);

        // Both id and name columns are present because of https://keboola.atlassian.net/browse/KBC-865
        $this->assertEquals($expectedRows, $rows);
    }

    protected function assertJobParamsMatches(array $expectedParams, $jobId)
    {
        $job = $this->clientWrapper->getBasicClient()->getJob($jobId);
        self::assertArraySubset($expectedParams, $job['operationParams']['params']);
    }
}
