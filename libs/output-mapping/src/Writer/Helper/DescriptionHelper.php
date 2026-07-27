<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

/**
 * The table/column description is stored in the native Storage description field, not as `KBC.description`
 * metadata (AJDA-2946). Storage itself mirrors the native value into a `KBC.description` metadata row under
 * the `storage` provider, so output mapping must not write the key as component metadata - it would create a
 * second, competing row.
 *
 * This helper keeps the key and the value normalisation in one place for both the resolution of the
 * description and its removal from the metadata structures.
 */
class DescriptionHelper
{
    public const DESCRIPTION_METADATA_KEY = 'KBC.description';

    /**
     * An empty description is treated as no description, so that it never clears a value stored in Storage.
     */
    public static function normalizeDescription(mixed $description): ?string
    {
        if (!is_scalar($description)) {
            return null;
        }

        $description = (string) $description;

        return $description !== '' ? $description : null;
    }

    /**
     * Removes the description from a key => value metadata map.
     *
     * @param mixed $metadata the node is a variableNode in the configuration, so it may be anything
     * @return array<string, mixed>
     */
    public static function removeDescriptionFromMetadataMap(mixed $metadata): array
    {
        if (!is_array($metadata)) {
            return [];
        }

        unset($metadata[self::DESCRIPTION_METADATA_KEY]);

        return $metadata;
    }

    /**
     * Removes the description from a list of {key, value} metadata items.
     *
     * @param array<mixed> $metadata
     * @return list<mixed>
     */
    public static function removeDescriptionFromMetadataList(array $metadata): array
    {
        return array_values(array_filter(
            $metadata,
            fn($item): bool => !is_array($item) || ($item['key'] ?? null) !== self::DESCRIPTION_METADATA_KEY,
        ));
    }
}
