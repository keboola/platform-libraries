<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;

class SqlWorkspaceTableStrategyTest extends AbstractWorkspaceTableStrategyTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->strategy = $this
            ->getWorkspaceStagingFactory(
                clientWrapper: $this->clientWrapper,
                workspaceId: 'fake-workspace-id',
            )
            ->getTableOutputStrategy();

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

        $mapping = $this->strategy->getMapping($configuration);
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

        self::assertFalse($this->strategy->hasDirectGrantUnloadStrategy());
        $this->strategy->getMapping($configuration);
        self::assertTrue($this->strategy->hasDirectGrantUnloadStrategy());
    }

    public function testGetMappingWithEmptyConfiguration(): void
    {
        $mapping = $this->strategy->getMapping([]);
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

        $mapping = $this->strategy->getMapping($configuration);
        self::assertCount(2, $mapping, 'All direct-grant configurations should be filtered out');
        self::assertTrue($this->strategy->hasDirectGrantUnloadStrategy());

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
