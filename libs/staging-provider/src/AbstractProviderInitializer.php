<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\Scope;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Staging\LocalStaging;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StagingProvider\WorkspaceProviderFactory\WorkspaceProviderFactoryInterface;

abstract class AbstractProviderInitializer
{
    public function __construct(
        private readonly AbstractStrategyFactory $stagingFactory,
        private readonly WorkspaceProviderFactoryInterface $workspaceProviderFactory,
        private readonly string $dataDirectory
    ) {
    }

    abstract public function initializeProviders(string $stagingType, array $tokenInfo): void;

    /**
     * @param class-string<WorkspaceStagingInterface> $workspaceClass
     * @param array<string, Scope> $scopes
     */
    protected function addWorkspaceProvider(string $workspaceClass, array $scopes): void
    {
        $stagingProvider = $this->workspaceProviderFactory->getProvider($workspaceClass);
        $this->stagingFactory->addProvider($stagingProvider, $scopes);
    }

    /**
     * @param array<string, Scope> $scopes
     */
    protected function addLocalProvider(array $scopes): void
    {
        $stagingProvider = new LocalStagingProvider(function () {
            return new LocalStaging($this->dataDirectory);
        });

        $this->stagingFactory->addProvider($stagingProvider, $scopes);
    }
}
