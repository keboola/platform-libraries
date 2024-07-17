<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Table\Options\InputTableOptions;

class BuildQueryFromConfigurationHelper
{
    public static function buildQuery(array $configuration): string
    {
        $tagsPresent = count($configuration['source']['tags'] ?? []) > 0;

        if (isset($configuration['query']) && $tagsPresent) {
            return sprintf(
                '%s AND (%s)',
                $configuration['query'],
                self::buildQueryForSourceTags($configuration['source']['tags']),
            );
        }
        if ($tagsPresent) {
            return self::buildQueryForSourceTags(
                $configuration['source']['tags'],
                $configuration['changed_since'] ?? null,
            );
        }
        return $configuration['query'];
    }

    public static function buildQueryForSourceTags(array $tags, ?string $changedSince = null): string
    {
        $query = implode(
            ' AND ',
            array_map(function (array $tag) {
                $queryPart = sprintf('tags:"%s"', $tag['name']);
                if ($tag['match'] === FakeDevStorageTagsRewriteHelper::MATCH_TYPE_EXCLUDE) {
                    $queryPart = 'NOT ' . $queryPart;
                }
                return $queryPart;
            }, $tags),
        );
        if ($changedSince && $changedSince !== InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE) {
            $query = '(' . $query . ') AND ' . self::getChangedSinceQueryPortion($changedSince);
        }
        return $query;
    }

    public static function getChangedSinceQueryPortion(string $changedSince): string
    {
        return sprintf(
            'created:["%s" TO *]',
            date('c', (int) strtotime($changedSince)),
        );
    }

    public static function getTagsFromSourceTags(array $tags): array
    {
        return array_map(function ($tag) {
            return $tag['name'];
        }, $tags);
    }

    public static function getSourceTagsFromTags(array $tags): array
    {
        return array_map(function ($tag) {
            return [
                'name' => $tag,
            ];
        }, $tags);
    }
}
