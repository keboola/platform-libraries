<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\StorageApiBranch\StorageApiToken;

class SqlWorkspaceTableStrategyTest extends AbstractWorkspaceTableStrategyTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken->method('hasFeature')->willReturn(false);
        $outputMappingSettings = new OutputMappingSettings(
            [],
            'upload',
            $storageApiToken,
            false,
            'none',
        );
        $this->strategy = $this
            ->getWorkspaceStagingFactory(
                clientWrapper: $this->clientWrapper,
                workspaceId: 'fake-workspace-id',
            )
            ->getTableOutputStrategy($outputMappingSettings);

        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $this->strategy);
    }

    public function testHasDirectGrantUnloadStrategyReturnsFalseInitially(): void
    {
        self::assertFalse($this->strategy->hasDirectGrantUnloadStrategy());
    }

    public function testGetMappingFiltersDirectGrantConfigurations(): void
    {
        $configuration = [
            'mapping' => [
                [
                    'source' => 'source1',
                    'destination' => 'destination1',
                ],
                [
                    'source' => 'source2',
                    'destination' => 'destination2',
                    'unload_strategy' => SqlWorkspaceTableStrategy::DIRECT_GRANT_UNLOAD_STRATEGY,
                ],
                [
                    'source' => 'source3',
                    'destination' => 'destination3',
                ],
            ],
        ];

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken->method('hasFeature')->willReturn(false);
        $outputMappingSettings = new OutputMappingSettings(
            $configuration,
            'upload',
            $storageApiToken,
            false,
            'none',
        );
        $strategy = $this
            ->getWorkspaceStagingFactory(
                clientWrapper: $this->clientWrapper,
                workspaceId: 'fake-workspace-id',
            )
            ->getTableOutputStrategy($outputMappingSettings);

        $mapping = $strategy->getMapping();
        self::assertCount(2, $mapping, 'Direct-grant configurations should be filtered out');

        $sourceNames = array_map(
            fn(MappingFromRawConfiguration $m) => $m->getSourceName(),
            $mapping,
        );
        self::assertContains('source1', $sourceNames);
        self::assertContains('source3', $sourceNames);
        self::assertNotContains('source2', $sourceNames);
    }

    public function testGetMappingSetsHasDirectGrantUnloadStrategyFlag(): void
    {
        $configuration = [
            'mapping' => [
                [
                    'source' => 'source1',
                    'destination' => 'destination1',
                    'unload_strategy' => SqlWorkspaceTableStrategy::DIRECT_GRANT_UNLOAD_STRATEGY,
                ],
            ],
        ];

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken->method('hasFeature')->willReturn(false);
        $outputMappingSettings = new OutputMappingSettings(
            $configuration,
            'upload',
            $storageApiToken,
            false,
            'none',
        );
        $strategy = $this
            ->getWorkspaceStagingFactory(
                clientWrapper: $this->clientWrapper,
                workspaceId: 'fake-workspace-id',
            )
            ->getTableOutputStrategy($outputMappingSettings);

        self::assertTrue($strategy->hasDirectGrantUnloadStrategy());
        $mapping = $strategy->getMapping();
        self::assertCount(0, $mapping);
    }

    public function testGetMappingWithEmptyConfiguration(): void
    {
        $mapping = $this->strategy->getMapping();
        self::assertCount(0, $mapping);
        self::assertFalse($this->strategy->hasDirectGrantUnloadStrategy());
    }

    public function testGetMappingWithMixedDirectGrantConfigurations(): void
    {
        $configuration = [
            'mapping' => [
                [
                    'source' => 'source1',
                    'destination' => 'destination1',
                ],
                [
                    'source' => 'source2',
                    'destination' => 'destination2',
                    'unload_strategy' => SqlWorkspaceTableStrategy::DIRECT_GRANT_UNLOAD_STRATEGY,
                ],
                [
                    'source' => 'source3',
                    'destination' => 'destination3',
                    'unload_strategy' => SqlWorkspaceTableStrategy::DIRECT_GRANT_UNLOAD_STRATEGY,
                ],
                [
                    'source' => 'source4',
                    'destination' => 'destination4',
                ],
            ],
        ];

        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken->method('hasFeature')->willReturn(false);
        $outputMappingSettings = new OutputMappingSettings(
            $configuration,
            'upload',
            $storageApiToken,
            false,
            'none',
        );
        $strategy = $this
            ->getWorkspaceStagingFactory(
                clientWrapper: $this->clientWrapper,
                workspaceId: 'fake-workspace-id',
            )
            ->getTableOutputStrategy($outputMappingSettings);

        $mapping = $strategy->getMapping();
        self::assertCount(2, $mapping, 'All direct-grant configurations should be filtered out');
        self::assertTrue($strategy->hasDirectGrantUnloadStrategy());

        $sourceNames = array_map(
            fn(MappingFromRawConfiguration $m) => $m->getSourceName(),
            $mapping,
        );
        self::assertContains('source1', $sourceNames);
        self::assertContains('source4', $sourceNames);
        self::assertNotContains('source2', $sourceNames);
        self::assertNotContains('source3', $sourceNames);
    }
}
