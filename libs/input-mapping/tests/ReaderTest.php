<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\InputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Finder\Finder;

class ReaderTest extends AbstractTestCase
{
    private function getStagingFactory(
        ClientWrapper $clientWrapper,
        string $format = 'json',
        ?LoggerInterface $logger = null,
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
        $mockLocal = $this->getMockBuilder(NullProvider::class)
            ->onlyMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            },
        );
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([
                    Scope::TABLE_DATA, Scope::TABLE_METADATA,
                    Scope::FILE_DATA, Scope::FILE_METADATA,
                ]),
            ],
        );
        return $stagingFactory;
    }

    public function testParentId(): void
    {
        $clientWrapper = $this->initClient();
        $clientWrapper->getTableAndFileStorageClient()->setRunId('123456789');
        self::assertEquals(
            '123456789',
            Reader::getParentRunId((string) $clientWrapper->getTableAndFileStorageClient()->getRunId()),
        );
        $clientWrapper->getTableAndFileStorageClient()->setRunId('123456789.98765432');
        self::assertEquals(
            '123456789',
            Reader::getParentRunId((string) $clientWrapper->getTableAndFileStorageClient()->getRunId()),
        );
        $clientWrapper->getTableAndFileStorageClient()->setRunId('123456789.98765432.4563456');
        self::assertEquals(
            '123456789.98765432',
            Reader::getParentRunId((string) $clientWrapper->getTableAndFileStorageClient()->getRunId()),
        );
        $clientWrapper->getTableAndFileStorageClient()->setRunId(null);
        self::assertEquals(
            '',
            Reader::getParentRunId((string) $clientWrapper->getTableAndFileStorageClient()->getRunId()),
        );
    }

    public function testReadInvalidConfiguration(): void
    {
        // empty configuration, ignored
        $reader = new Reader($this->getStagingFactory($this->initClient()));
        $configuration = new InputTableOptionsList([]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadTablesDefaultBackend(): void
    {
        $reader = new Reader($this->getStagingFactory($this->initClient()));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'not-needed.test',
                'destination' => 'test.csv',
            ],
        ]);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Input mapping on type "invalid" is not supported. Supported types are "abs, local, s3, ',
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            'invalid',
            new ReaderOptions(true),
        );
    }

    #[NeedsDevBranch]
    public function testReadTablesDefaultBackendBranchRewrite(): void
    {
        $clientWrapper = $this->initClient();

        file_put_contents($this->temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($this->temp->getTmpFolder() . 'data.csv');

        $branchBucket = TestSatisfyer::getBucketByDisplayName(
            $clientWrapper,
            'my-branch-input-mapping-test',
            Client::STAGE_IN,
        );
        if ($branchBucket) {
            $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                $branchBucket['id'],
                ['force' => true, 'async' => true],
            );
        }
        $inBucket = TestSatisfyer::getBucketByDisplayName(
            $clientWrapper,
            'input-mapping-test',
            Client::STAGE_IN,
        );
        if ($inBucket) {
            $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                $inBucket['id'],
                ['force' => true, 'async' => true],
            );
        }
        foreach ($clientWrapper->getTableAndFileStorageClient()->listBuckets() as $bucket) {
            if (preg_match('/^(c-)?[0-9]+-input-mapping-test/ui', $bucket['name'])) {
                $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                    $bucket['id'],
                    ['force' => true, 'async' => true],
                );
            }
        }

        $inBucketId = $clientWrapper->getTableAndFileStorageClient()->createBucket(
            'input-mapping-test',
            Client::STAGE_IN,
        );
        // we need to know the $inBucketId, which is known only after creation, but we need the bucket not to exist
        // hence - create the bucket, get it id, and drop it
        $clientWrapper->getTableAndFileStorageClient()->dropBucket($inBucketId, ['force' => true, 'async' => true]);
        $branchBucketId = $clientWrapper->getTableAndFileStorageClient()->createBucket(
            sprintf('%s-input-mapping-test', $this->devBranchId),
            Client::STAGE_IN,
        );
        $clientWrapper->getTableAndFileStorageClient()->createTableAsync($branchBucketId, 'test', $csvFile);
        $reader = new Reader($this->getStagingFactory($this->initClient($this->devBranchId)));
        $configuration = new InputTableOptionsList([
            [
                'source' => $inBucketId . '.test',
                'destination' => 'test.csv',
            ],
        ]);
        $state = new InputTableStateList([
            [
                'source' => $inBucketId . '.test',
                'lastImportDate' => '1605741600',
            ],
        ]);

        $result = $reader->downloadTables(
            $configuration,
            $state,
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        self::assertStringContainsString(
            "\"foo\",\"bar\"\n\"1\",\"2\"",
            (string) file_get_contents($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv'),
        );
        $data = $result->getInputTableStateList()->jsonSerialize();
        self::assertEquals(sprintf('%s.test', $branchBucketId), $data[0]['source']);
        self::assertArrayHasKey('lastImportDate', $data[0]);
    }
}
