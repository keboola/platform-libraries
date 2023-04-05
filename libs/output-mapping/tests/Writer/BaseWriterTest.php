<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use PHPUnit\Util\Test;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseWriterTest extends TestCase
{
    protected ClientWrapper $clientWrapper;
    protected Temp $tmp;

    protected function clearBuckets(array $buckets): void
    {
        foreach ($buckets as $bucket) {
            try {
                $this->clientWrapper->getBasicClient()->dropBucket($bucket, ['force' => true, 'async' => false]);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }
    }

    protected function clearFileUploads(array $tags): void
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

    public function setUp(): void
    {
        parent::setUp();
        $this->tmp = new Temp();
        $root = $this->tmp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'download');
        $this->initClient();
    }

    protected function initClient(?string $branchId = null): void
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('STORAGE_API_URL'))
            ->setToken((string) getenv('STORAGE_API_TOKEN'))
            ->setBranchId($branchId)
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            })
            ->setUserAgent(implode('::', Test::describe($this)));
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

    protected function getStagingFactory(
        ?ClientWrapper $clientWrapper = null,
        string $format = 'json',
        ?LoggerInterface $logger = null
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ?: $this->clientWrapper,
            $logger ?: new NullLogger(),
            $format
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function (): string {
                return $this->tmp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([
                    Scope::TABLE_DATA, Scope::TABLE_METADATA,
                    Scope::FILE_DATA, Scope::FILE_METADATA,
                ]),
            ]
        );
        return $stagingFactory;
    }

    protected function assertTablesExists(string $bucketId, array $expectedTables): void
    {
        $tables = $this->clientWrapper->getBasicClient()->listTables($bucketId);
        $tableIds = array_column($tables, 'id');

        // order of listed tables is not guaranteed
        sort($tableIds);
        sort($expectedTables);

        self::assertSame($expectedTables, $tableIds);
    }

    protected function assertTableRowsEquals(string $tableName, array $expectedRows): void
    {
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview($tableName);

        $rows = explode("\n", trim($data));
        // convert to lowercase because of https://keboola.atlassian.net/browse/KBC-864
        $rows = array_map('strtolower', $rows);

        // order of rows is not guaranteed
        $rows = sort($rows);
        $expectedRows = sort($expectedRows);

        // Both id and name columns are present because of https://keboola.atlassian.net/browse/KBC-865
        self::assertEquals($expectedRows, $rows);
    }

    protected function assertJobParamsMatches(array $expectedParams, string $jobId): void
    {
        $job = $this->clientWrapper->getBasicClient()->getJob($jobId);
        foreach ($expectedParams as $expectedParam) {
            self::assertContains($expectedParam, $job['operationParams']['params']);
        }
    }

    protected static function assertTableImportJob(array $jobData, bool $expectedIncrementalFlag): void
    {
        self::assertSame('tableImport', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedIncrementalFlag, $jobData['operationParams']['params']['incremental']);
        self::assertSame([], $jobData['results']['newColumns']);
    }

    protected static function assertTableColumnAddJob(
        array $jobData,
        string $expectedColumnName
    ): void {
        self::assertSame('tableColumnAdd', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedColumnName, $jobData['operationParams']['name']);
        self::assertArrayNotHasKey('basetype', $jobData['operationParams']);
        self::assertArrayNotHasKey('definition', $jobData['operationParams']);
    }

    public function incrementalFlagProvider(): Generator
    {
        yield 'incremental load' => [true];
        yield 'full load' => [false];
    }
}
