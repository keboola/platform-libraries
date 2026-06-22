<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;

/**
 * Updates table and column descriptions on an already existing table through the Storage table-definition
 * update endpoint (PUT /tables/{id}/definition).
 *
 * A description is only updated when no user has set it: Storage stores KBC.description once per provider and
 * the `user` provider row (metadata API / UI edit) wins. If such a row exists for the table or column, the
 * component must not overwrite it, so it is skipped. Otherwise the configured description is written to the
 * native field.
 */
class TableDescriptionUpdater
{
    private const DESCRIPTION_METADATA_KEY = 'KBC.description';
    private const USER_PROVIDER = 'user';

    public function __construct(
        private readonly ClientWrapper $clientWrapper,
    ) {
    }

    public function updateDescriptions(TableInfo $tableInfo, MappingFromProcessedConfiguration $source): void
    {
        $payload = [];

        $tableDescription = $source->getTableDescription();
        if ($tableDescription !== null && !$this->hasUserDescription($tableInfo->getMetadata())) {
            $payload['description'] = $tableDescription;
        }

        $columnMetadata = $tableInfo->getColumnMetadata();
        $columns = [];
        foreach ($source->getColumnDescriptions() as $columnName => $description) {
            if ($this->hasUserDescription($columnMetadata[$columnName] ?? [])) {
                continue;
            }
            $columns[] = [
                'name' => $columnName,
                'description' => $description,
            ];
        }
        if ($columns) {
            $payload['columns'] = $columns;
        }

        if (!$payload) {
            return;
        }

        try {
            $this->clientWrapper->getTableAndFileStorageClient()->updateTableDefinition(
                $tableInfo->getId(),
                $payload,
            );
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf('Cannot update description of table "%s": %s', $tableInfo->getId(), $e->getMessage()),
                $e->getCode(),
                $e,
            );
        }
    }

    /**
     * @param array<array{key?: string, value?: string, provider?: string}> $metadata
     */
    private function hasUserDescription(array $metadata): bool
    {
        foreach ($metadata as $item) {
            if (($item['key'] ?? null) === self::DESCRIPTION_METADATA_KEY
                && ($item['provider'] ?? null) === self::USER_PROVIDER
            ) {
                return true;
            }
        }
        return false;
    }
}
