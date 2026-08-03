<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;

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
     * @param string $configurationNode name of the configuration node, for the error message
     * @return array<string, mixed>
     */
    public static function removeDescriptionFromMetadataMap(mixed $metadata, string $configurationNode): array
    {
        $metadata = self::assertMetadataMap($metadata, $configurationNode);

        unset($metadata[self::DESCRIPTION_METADATA_KEY]);

        return $metadata;
    }

    /**
     * Reads the description from a key => value metadata map.
     *
     * @param mixed $metadata the node is a variableNode in the configuration, so it may be anything
     * @param string $configurationNode name of the configuration node, for the error message
     */
    public static function getDescriptionFromMetadataMap(mixed $metadata, string $configurationNode): ?string
    {
        $metadata = self::assertMetadataMap($metadata, $configurationNode);

        return isset($metadata[self::DESCRIPTION_METADATA_KEY])
            ? self::normalizeDescription($metadata[self::DESCRIPTION_METADATA_KEY])
            : null;
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
            fn($item): bool => !self::isDescriptionItem($item),
        ));
    }

    /**
     * Reads the description from a list of {key, value} metadata items. The first matching item decides, the
     * schema does not prevent a list from carrying more than one.
     *
     * @param array<mixed> $metadata
     */
    public static function getDescriptionFromMetadataList(array $metadata): ?string
    {
        foreach ($metadata as $item) {
            if (self::isDescriptionItem($item)) {
                return self::normalizeDescription($item['value'] ?? null);
            }
        }

        return null;
    }

    /**
     * @phpstan-assert-if-true array{key: string, value?: mixed} $item
     */
    private static function isDescriptionItem(mixed $item): bool
    {
        return is_array($item) && ($item['key'] ?? null) === self::DESCRIPTION_METADATA_KEY;
    }

    /**
     * A metadata map comes from a variableNode, so the configuration cannot guarantee its type. A value which
     * is not a map is a configuration error and must be reported, not silently dropped.
     *
     * @return array<string, mixed>
     */
    private static function assertMetadataMap(mixed $metadata, string $configurationNode): array
    {
        if (!is_array($metadata)) {
            throw new InvalidOutputException(sprintf(
                'Configuration node "%s" must be an object, "%s" given.',
                $configurationNode,
                get_debug_type($metadata),
            ));
        }

        /** @var array<string, mixed> $metadata */
        return $metadata;
    }
}
