<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\Metadata;

use Keboola\StorageApi\Metadata;

interface MetadataInterface
{
    public function apply(Metadata $apiClient): void;
}
