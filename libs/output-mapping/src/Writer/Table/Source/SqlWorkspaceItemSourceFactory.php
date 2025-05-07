<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Source;

use Keboola\InputMapping\Staging\StagingInterface;

final class SqlWorkspaceItemSourceFactory implements WorkspaceItemSourceFactoryInterface
{
    public function __construct(private readonly StagingInterface $dataStorage)
    {
    }

    public function createSource(string $sourcePathPrefix, string $sourceName): WorkspaceItemSource
    {
        return new WorkspaceItemSource(
            $sourceName,
            $this->dataStorage->getWorkspaceId(),
            $sourceName,
            false,
        );
    }
}
