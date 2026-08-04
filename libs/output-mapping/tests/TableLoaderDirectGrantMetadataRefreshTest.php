<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Generator;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
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
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

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

    private function uploadTables(BranchAwareClient&MockObject $branchClient): LoadTableQueue
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($branchClient);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($this->createMock(Client::class));

        $tableLoader = new TableLoader(
            new NullLogger(),
            $clientWrapper,
            $this->createStrategyFactory(),
        );

        return $tableLoader->uploadTables(
            new OutputMappingSettings(
                ['mapping' => [['source' => 'table1a', 'unload_strategy' => 'direct-grant']]],
                'upload',
                new StorageApiToken(['owner' => ['features' => []]], 'token', AuthType::STORAGE_TOKEN),
                false,
                OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
            ),
            new SystemMetadata(['componentId' => 'testComponent']),
        );
    }

    private function createStrategyFactory(): StrategyFactory&MockObject
    {
        $dataStorage = $this->createMock(WorkspaceStagingInterface::class);
        $dataStorage->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn(self::WORKSPACE_ID)
        ;

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects(self::atMost(1))
            ->method('getPath')
            ->willReturn('/tmp/output-mapping-metadata-does-not-exist')
        ;

        $mappingCombiner = $this->createMock(MappingCombinerInterface::class);
        $mappingCombiner->expects(self::once())
            ->method('combineDataItemsWithConfigurations')
            ->willReturn([])
        ;
        $mappingCombiner->expects(self::once())
            ->method('combineSourcesWithManifests')
            ->willReturn([])
        ;

        $strategy = $this->createMock(SqlWorkspaceTableStrategy::class);
        $strategy->method('getSourcesValidator')->willReturn($this->createMock(SourcesValidatorInterface::class));
        $strategy->method('getMappingCombiner')->willReturn($mappingCombiner);
        $strategy->method('getMapping')->willReturn([]);
        $strategy->method('listSources')->willReturn([]);
        $strategy->method('listManifests')->willReturn([]);
        $strategy->method('hasSlicer')->willReturn(false);
        $strategy->method('hasDirectGrantUnloadStrategy')->willReturn(true);
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
