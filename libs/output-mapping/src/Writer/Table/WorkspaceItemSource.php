<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

class WorkspaceItemSource implements SourceInterface
{
    public function __construct(
        private readonly string $sourceName,
        private readonly string $workspaceId,
        private readonly string $dataObject,
        private readonly bool $isSliced,
    ) {
    }

    public function getName(): string
    {
        return $this->sourceName;
    }

    public function getWorkspaceId(): string
    {
        return $this->workspaceId;
    }

    public function getDataObject(): string
    {
        return $this->dataObject;
    }

    public function isSliced(): bool
    {
        return $this->isSliced;
    }
}
