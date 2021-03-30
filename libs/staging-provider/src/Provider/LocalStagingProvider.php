<?php

namespace Keboola\WorkspaceProvider\Provider;

use Keboola\WorkspaceProvider\Exception\StagingProviderException;
use Keboola\WorkspaceProvider\Staging\LocalStaging;

/**
 * @extends AbstractStagingProvider<LocalStagingProvider>
 */
class LocalStagingProvider extends AbstractStagingProvider
{
    public function __construct(callable $stagingGetter)
    {
        parent::__construct($stagingGetter, LocalStaging::class);
    }

    public function getWorkspaceId()
    {
        throw new StagingProviderException('Local staging provider does not support workspace ID.');
    }

    public function getCredentials()
    {
        throw new StagingProviderException('Local staging provider does not support workspace credentials.');
    }

    public function getPath()
    {
        return $this->getStaging()->getPath();
    }

    public function cleanup()
    {
        // do nothing
    }
}
