<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\OutputMapping\DeferredTasks\TableDefinition\TableDefinitionDescription;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

interface LoadTableTaskInterface
{
    public function startImport(Client $client): void;

    public function applyMetadata(Metadata $metadataApiClient): void;

    public function getMetadata(): array;

    public function getDestinationTableName(): string;

    public function getStorageJobId(): string;

    public function isUsingFreshlyCreatedTable(): bool;

    public function addMetadata(MetadataInterface $metadataDefinition): void;

    public function setDescription(TableDefinitionDescription $description): void;

    public function applyDescription(Client $client): void;
}
