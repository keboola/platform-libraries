<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Generator;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\MappingCombiner\MappingCombinerInterface;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SourcesValidator\SourcesValidatorInterface;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\TableLoader;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\StorageApiToken;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;

class TableLoaderDirectGrantMetadataRefreshTest extends TestCase
{
    private const WORKSPACE_ID = '1234';
    private const UNLOAD_URL = 'workspaces/' . self::WORKSPACE_ID . '/unload?only-direct-grants=1';

    public function testRefreshJobIsAwaitedByTheQueue(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willReturn([['id' => '456']])
        ;
        $branchClient->expects(self::once())
            ->method('waitForJob')
            ->with('456')
            ->willReturn([
                'operationName' => 'refreshStorageBuckets',
                'status' => 'success',
            ])
        ;

        $tableQueue = $this->uploadTables($branchClient);

        self::assertSame(1, $tableQueue->getTaskCount());
        self::assertSame(['456'], $tableQueue->waitForAll());
    }

    public function testEveryRefreshJobReturnedByStorageIsAwaited(): void
    {
        $awaitedJobIds = [];

        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willReturn([['id' => '456'], ['id' => '789']])
        ;
        $branchClient->expects(self::exactly(2))
            ->method('waitForJob')
            ->willReturnCallback(function ($jobId) use (&$awaitedJobIds): array {
                $awaitedJobIds[] = $jobId;
                return ['operationName' => 'refreshStorageBuckets', 'status' => 'success'];
            })
        ;

        $tableQueue = $this->uploadTables($branchClient);

        self::assertSame(2, $tableQueue->getTaskCount());
        self::assertSame(['456', '789'], $tableQueue->waitForAll());
        self::assertSame(['456', '789'], $awaitedJobIds);
    }

    public function testFailedRefreshJobFailsTheQueue(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willReturn([['id' => '456']])
        ;
        $branchClient->expects(self::once())
            ->method('waitForJob')
            ->with('456')
            ->willReturn([
                'operationName' => 'refreshStorageBuckets',
                'status' => 'error',
                'error' => [
                    'message' => 'Workspace "1234" not found.',
                ],
            ])
        ;

        $tableQueue = $this->uploadTables($branchClient);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Failed to refresh metadata of direct-grant tables (Storage job "456"): Workspace "1234" not found.',
        );
        $tableQueue->waitForAll();
    }

    public function testRefreshFailureWithSapiUserErrorThrowsInvalidOutputException(): void
    {
        $clientException = new ClientException('Workspace "1234" not found.', 404);

        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willThrowException($clientException)
        ;

        try {
            $this->uploadTables($branchClient);
            self::fail('Upload should fail with InvalidOutputException.');
        } catch (InvalidOutputException $e) {
            self::assertSame(
                'Failed to refresh metadata of direct-grant tables: Workspace "1234" not found.',
                $e->getMessage(),
            );
            self::assertSame(404, $e->getCode());
            self::assertSame($clientException, $e->getPrevious());
        }
    }

    public static function nonUserErrorProvider(): Generator
    {
        yield 'server error' => ['Internal Server Error', 500];
        yield 'connection failure' => ['cURL error 6: Could not resolve host', 0];
    }

    /**
     * @dataProvider nonUserErrorProvider
     */
    public function testRefreshFailureWithSapiAppErrorPropagatesErrorFromClient(string $message, int $code): void
    {
        $clientException = new ClientException($message, $code);

        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willThrowException($clientException)
        ;

        try {
            $this->uploadTables($branchClient);
            self::fail('Upload should fail with ClientException.');
        } catch (ClientException $e) {
            self::assertSame($clientException, $e);
        }
    }

    public function testFailedJobRefreshesEvenWhenListingSourcesFails(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willReturn([['id' => '456']])
        ;
        $branchClient->expects(self::once())
            ->method('waitForJob')
            ->with('456')
            ->willReturn(['operationName' => 'refreshStorageBuckets', 'status' => 'success'])
        ;

        $sourcesValidator = $this->createMock(SourcesValidatorInterface::class);
        $sourcesValidator->expects(self::once())
            ->method('validatePhysicalFilesWithManifest')
            ->willThrowException(new InvalidOutputException('Table sources not found: "table1a"'))
        ;

        $logsHandler = new TestHandler();
        $tableLoader = $this->createTableLoader(
            $branchClient,
            $this->createStrategyFactory(processesSources: false, sourcesValidator: $sourcesValidator),
            new Logger('test', [$logsHandler]),
        );
        $tableQueue = $tableLoader->uploadTables(
            $this->createSettings(isFailedJob: true),
            new SystemMetadata(['componentId' => 'testComponent']),
        );

        self::assertSame(['456'], $tableQueue->waitForAll());
        self::assertTrue($logsHandler->hasWarning(
            'Failed to list output tables: Table sources not found: "table1a"',
        ));
    }

    public function testFailedJobRefreshesEvenWhenLoadingTableFails(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willReturn([['id' => '456']])
        ;
        $branchClient->expects(self::once())
            ->method('waitForJob')
            ->with('456')
            ->willReturn(['operationName' => 'refreshStorageBuckets', 'status' => 'success'])
        ;

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $source->method('getSourceName')->willReturn('table1b');
        $source->method('getConfiguration')->willThrowException(new RuntimeException('Something broke.'));

        $logsHandler = new TestHandler();
        $tableLoader = $this->createTableLoader(
            $branchClient,
            $this->createStrategyFactory(sources: [$source]),
            new Logger('test', [$logsHandler]),
        );
        $tableQueue = $tableLoader->uploadTables(
            $this->createSettings(isFailedJob: true),
            new SystemMetadata(['componentId' => 'testComponent']),
        );

        self::assertSame([], $tableQueue->getLoadTableTasks());
        self::assertSame(['456'], $tableQueue->waitForAll());
        self::assertTrue($logsHandler->hasWarning('Failed to load table "table1b": Something broke.'));
    }

    public function testSucceededJobFailingOnSourcesDoesNotRefresh(): void
    {
        // the runner retries the output mapping as a failed job, which does the refresh
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::never())->method('apiPostJson');

        $sourcesValidator = $this->createMock(SourcesValidatorInterface::class);
        $sourcesValidator->expects(self::once())
            ->method('validatePhysicalFilesWithManifest')
            ->willThrowException(new InvalidOutputException('Table sources not found: "table1a"'))
        ;

        $tableLoader = $this->createTableLoader(
            $branchClient,
            $this->createStrategyFactory(processesSources: false, sourcesValidator: $sourcesValidator),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "table1a"');
        $tableLoader->uploadTables($this->createSettings(), new SystemMetadata(['componentId' => 'testComponent']));
    }

    public function testRefreshDirectGrantMetadataRefreshesWithoutLoadingTables(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::once())
            ->method('apiPostJson')
            ->with(self::UNLOAD_URL, [], false)
            ->willReturn([['id' => '456']])
        ;
        $branchClient->expects(self::once())
            ->method('waitForJob')
            ->with('456')
            ->willReturn(['operationName' => 'refreshStorageBuckets', 'status' => 'success'])
        ;

        $tableLoader = $this->createTableLoader(
            $branchClient,
            $this->createStrategyFactory(processesSources: false),
        );
        $tableQueue = $tableLoader->refreshDirectGrantMetadata($this->createSettings());

        self::assertSame(1, $tableQueue->getTaskCount());
        self::assertSame(['456'], $tableQueue->waitForAll());
    }

    public function testRefreshDirectGrantMetadataWithoutDirectGrantTablesDoesNothing(): void
    {
        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->expects(self::never())->method('apiPostJson');
        $branchClient->expects(self::never())->method('waitForJob');

        $tableLoader = $this->createTableLoader(
            $branchClient,
            $this->createStrategyFactory(hasDirectGrantUnloadStrategy: false, processesSources: false),
        );
        $tableQueue = $tableLoader->refreshDirectGrantMetadata($this->createSettings());

        self::assertSame(0, $tableQueue->getTaskCount());
        self::assertSame([], $tableQueue->waitForAll());
    }

    private function uploadTables(BranchAwareClient&MockObject $branchClient): LoadTableQueue
    {
        return $this->createTableLoader($branchClient, $this->createStrategyFactory())->uploadTables(
            $this->createSettings(),
            new SystemMetadata(['componentId' => 'testComponent']),
        );
    }

    private function createTableLoader(
        BranchAwareClient&MockObject $branchClient,
        StrategyFactory&MockObject $strategyFactory,
        LoggerInterface $logger = new NullLogger(),
    ): TableLoader {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($branchClient);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($this->createMock(Client::class));

        return new TableLoader($logger, $clientWrapper, $strategyFactory);
    }

    private function createSettings(bool $isFailedJob = false): OutputMappingSettings
    {
        return new OutputMappingSettings(
            ['mapping' => [['source' => 'table1a', 'unload_strategy' => 'direct-grant']]],
            'upload',
            new StorageApiToken(['owner' => ['features' => []]], 'token', AuthType::STORAGE_TOKEN),
            $isFailedJob,
            OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
        );
    }

    /**
     * @param MappingFromRawConfigurationAndPhysicalDataWithManifest[] $sources
     */
    private function createStrategyFactory(
        bool $hasDirectGrantUnloadStrategy = true,
        bool $processesSources = true,
        ?SourcesValidatorInterface $sourcesValidator = null,
        array $sources = [],
    ): StrategyFactory&MockObject {
        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects($hasDirectGrantUnloadStrategy ? self::once() : self::never())
            ->method('getWorkspaceId')
            ->willReturn(self::WORKSPACE_ID)
        ;

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects(self::atMost(1))
            ->method('getPath')
            ->willReturn('/tmp/output-mapping-metadata-does-not-exist')
        ;

        $mappingCombiner = $this->createMock(MappingCombinerInterface::class);
        $mappingCombiner->expects($processesSources ? self::once() : self::never())
            ->method('combineDataItemsWithConfigurations')
            ->willReturn([])
        ;
        $mappingCombiner->expects($processesSources ? self::once() : self::never())
            ->method('combineSourcesWithManifests')
            ->willReturn($sources)
        ;

        $strategy = $this->createMock(SqlWorkspaceTableStrategy::class);
        $strategy->method('getSourcesValidator')
            ->willReturn($sourcesValidator ?? $this->createMock(SourcesValidatorInterface::class));
        $strategy->method('getMappingCombiner')->willReturn($mappingCombiner);
        $strategy->method('getMapping')->willReturn([]);
        $strategy->method('listSources')->willReturn([]);
        $strategy->method('listManifests')->willReturn([]);
        $strategy->method('hasSlicer')->willReturn(false);
        $strategy->method('hasDirectGrantUnloadStrategy')->willReturn($hasDirectGrantUnloadStrategy);
        $strategy->method('getDataStorage')->willReturn($dataStorage);
        $strategy->method('getMetadataStorage')->willReturn($metadataStorage);

        $strategyFactory = $this->createMock(StrategyFactory::class);
        $strategyFactory->expects(self::once())
            ->method('getTableOutputStrategy')
            ->willReturn($strategy)
        ;

        return $strategyFactory;
    }
}
