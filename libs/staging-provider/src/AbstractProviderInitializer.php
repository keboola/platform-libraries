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
    
    /** @var string */
    private $dataDirectory;

    public function __construct(
        InputStrategyFactory $stagingFactory,
        WorkspaceProviderFactoryInterface $workspaceProviderFactory,
        $dataDirectory
    ) {
        $this->stagingFactory = $stagingFactory;
        $this->workspaceProviderFactory = $workspaceProviderFactory;
        $this->dataDirectory = $dataDirectory;
    }

    /**
     * @param string $stagingType
     * @param array $tokenInfo
     */
    abstract public function initializeProviders($stagingType, array $tokenInfo);

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
     * @param array<string, Scope> $scopes
     */
    protected function addLocalProvider($scopes)
    {
        $stagingProvider = new LocalStagingProvider(function () {
            return new LocalStaging($this->dataDirectory);
        });

        $this->stagingFactory->addProvider($stagingProvider, $scopes);
    }
}
