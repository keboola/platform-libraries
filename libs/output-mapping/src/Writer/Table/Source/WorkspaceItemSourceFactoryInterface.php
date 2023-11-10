<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Source;

interface WorkspaceItemSourceFactoryInterface
{
    public function createSource(string $sourcePathPrefix, string $sourceName): WorkspaceItemSource;
}
