<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\LocalStaging;

/**
 * @extends AbstractStagingProvider<LocalStaging>
 */
class LocalStagingProvider extends AbstractStagingProvider
{
    /**
     * @param callable(): LocalStaging $stagingGetter
     */
    public function __construct(callable $stagingGetter)
    {
        parent::__construct($stagingGetter);
    }

    public function getWorkspaceId(): string
    {
        throw new StagingProviderException('Local staging provider does not support workspace ID.');
    }

    public function getCredentials(): array
    {
        throw new StagingProviderException('Local staging provider does not support workspace credentials.');
    }

    public function getPath(): string
    {
        return $this->getStaging()->getPath();
    }

    public function cleanup(): void
    {
        // do nothing
    }
}
