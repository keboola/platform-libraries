<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use LogicException;

abstract class AbstractLoadTableTask implements LoadTableTaskInterface
{
    protected MappingDestination $destination;
    protected array $options;
    protected bool $freshlyCreatedTable;
    protected string $storageJobId;
    /** @var MetadataInterface[] */
    protected array $metadata = [];

    public function __construct(MappingDestination $destination, array $options, bool $freshlyCreatedTable)
    {
        $this->destination = $destination;
        $this->options = $options;
        $this->freshlyCreatedTable = $freshlyCreatedTable;
    }

    abstract protected function queueStorageJob(Client $client): string;

    public function start(ClientWrapper $clientWrapper): void
    {
        try {
            $this->storageJobId = $this->queueStorageJob($clientWrapper->getTableAndFileStorageClient());
        } catch (ClientException $e) {
            if ($e->getCode() < 500) {
                throw new InvalidOutputException(
                    sprintf('%s [%s]', $e->getMessage(), $this->getDestinationTableName()),
                    $e->getCode(),
                    $e,
                );
            }

            throw $e;
        }
    }

    public function addMetadata(MetadataInterface $metadataDefinition): void
    {
        $this->metadata[] = $metadataDefinition;
    }

    public function getMetadata(): array
    {
        return $this->metadata;
    }

    public function applyMetadata(Metadata $metadataApiClient): void
    {
        foreach ($this->metadata as $metadataDefinition) {
            $metadataDefinition->apply($metadataApiClient);
        }
    }

    public function getDestinationTableName(): string
    {
        return $this->destination->getTableId();
    }

    public function getStorageJobIds(): array
    {
        if (!isset($this->storageJobId)) {
            throw new LogicException(sprintf(
                'Load of table "%s" has not been started yet.',
                $this->getDestinationTableName(),
            ));
        }

        return [$this->storageJobId];
    }

    public function getFailedJobError(string $storageJobId, array $jobResult): string
    {
        return sprintf(
            'Failed to load table "%s": %s',
            $this->getDestinationTableName(),
            $jobResult['error']['message'],
        );
    }

    public function isUsingFreshlyCreatedTable(): bool
    {
        return $this->freshlyCreatedTable;
    }
}
