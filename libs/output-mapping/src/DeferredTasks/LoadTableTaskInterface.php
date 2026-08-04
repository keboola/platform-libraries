<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\StorageApi\Metadata;

/**
 * A Storage job which writes a single destination table.
 */
interface LoadTableTaskInterface extends DeferredTaskInterface
{
    public function applyMetadata(Metadata $metadataApiClient): void;

    public function getMetadata(): array;

    public function getDestinationTableName(): string;

    public function isUsingFreshlyCreatedTable(): bool;

    public function addMetadata(MetadataInterface $metadataDefinition): void;
}
