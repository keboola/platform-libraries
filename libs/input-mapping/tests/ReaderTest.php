<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadQueue;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\InputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Finder\Finder;

class ReaderTest extends AbstractTestCase
{
    use ReflectionPropertyAccessTestCase;

    private function getStagingFactory(
        ClientWrapper $clientWrapper,
        StagingType $stagingType = StagingType::Local,
        FileFormat $format = FileFormat::Json,
        ?LoggerInterface $logger = null,
    ): StrategyFactory {
        $localStaging = $this->createMock(FileStagingInterface::class);
        $localStaging->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            },
        );

        return new StrategyFactory(
            new StagingProvider(
                stagingType: $stagingType,
                localStagingPath: $this->temp->getTmpFolder(),
                stagingWorkspaceId: null,
            ),
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
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
        $clientWrapper = $this->initClient();
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getStagingFactory($clientWrapper),
        );
        $configuration = new InputTableOptionsList([]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            new ReaderOptions(true),
        );
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
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

        $clientWrapper = $this->initClient($this->devBranchId);
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getStagingFactory($clientWrapper),
        );

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

    public function testPrepareAndExecuteTableLoadsWithNonWorkspaceStrategy(): void
    {
        $clientWrapper = $this->initClient();
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getStagingFactory($clientWrapper, StagingType::Local),
        );

        $configuration = new InputTableOptionsList([]);
        $state = new InputTableStateList([]);

        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('prepareAndExecuteTableLoads() can only be used with workspace strategies');

        $reader->prepareAndExecuteTableLoads(
            $configuration,
            $state,
            'destination',
            new ReaderOptions(true),
        );
    }

    /**
     * @dataProvider preserveFlagProvider
     */
    public function testPrepareAndExecuteTableLoadsDelegatesToWorkspaceStrategy(bool $preserveFlag): void
    {
        // Arrange
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getTableAndFileStorageClient')
            ->willReturn($this->createMock(Client::class));
        $clientWrapper->method('getClientOptionsReadOnly')
            ->willReturn($this->createMock(ClientOptions::class));

        $expectedQueue = new WorkspaceLoadQueue([]);

        $workspaceStrategy = $this->createMock(AbstractWorkspaceStrategy::class);
        $workspaceStrategy
            ->expects(self::once())
            ->method('prepareAndExecuteTableLoads')
            ->with([], $preserveFlag)
            ->willReturn($expectedQueue);

        $strategyFactory = $this->createMock(StrategyFactory::class);
        $strategyFactory
            ->expects(self::once())
            ->method('getTableInputStrategy')
            ->willReturn($workspaceStrategy);

        // Act
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $strategyFactory,
        );

        $result = $reader->prepareAndExecuteTableLoads(
            new InputTableOptionsList([]),
            new InputTableStateList([]),
            'destination',
            new ReaderOptions(false, $preserveFlag),
        );

        // Assert
        self::assertSame($expectedQueue, $result);
    }

    public function preserveFlagProvider(): Generator
    {
        yield 'with preserve enabled' => [true];
        yield 'with preserve disabled' => [false];
    }
}
