<?php

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Staging\LocalStaging;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StagingProvider\WorkspaceProviderFactory\WorkspaceProviderFactoryInterface;

abstract class AbstractProviderInitializer
{
    /** @var InputStrategyFactory */
    private $stagingFactory;

    /** @var WorkspaceProviderFactoryInterface */
    private $workspaceProviderFactory;

    public function __construct(
        InputStrategyFactory $stagingFactory,
        WorkspaceProviderFactoryInterface $workspaceProviderFactory
    ) {
        $this->stagingFactory = $stagingFactory;
        $this->workspaceProviderFactory = $workspaceProviderFactory;
    }

    /**
     * @param string $stagingType
     * @param array $tokenInfo
     * @param string $dataDirectory
     */
    abstract public function initializeProviders($stagingType, array $tokenInfo, $dataDirectory);

    /**
     * @param class-string<WorkspaceStagingInterface> $workspaceClass
     * @param array<string, Scope> $scopes
     */
    protected function addWorkspaceProvider($workspaceClass, $scopes)
    {
        $stagingProvider = $this->workspaceProviderFactory->getProvider($workspaceClass);
        $this->stagingFactory->addProvider($stagingProvider, $scopes);
    }

    /**
     * @param string $dataDirectory
     * @param array<string, Scope> $scopes
     */
    protected function addLocalProvider($dataDirectory, $scopes)
    {
        $stagingProvider = new LocalStagingProvider(function () use ($dataDirectory) {
            return new LocalStaging($dataDirectory);
        });

        $this->stagingFactory->addProvider($stagingProvider, $scopes);
    }
}
