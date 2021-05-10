<?php

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Table\Options\InputTableOptions;

class BuildQueryFromConfigurationHelper
{
    public static function buildQuery($configuration)
    {
        if (isset($configuration['query']) && isset($configuration['source']['tags'])) {
            return sprintf(
                '%s AND (%s)',
                $configuration['query'],
                self::buildQueryForSourceTags($configuration['source']['tags'])
            );
        }
        if (isset($configuration['source']['tags'])) {
            return self::buildQueryForSourceTags(
                $configuration['source']['tags'],
                isset($configuration['changed_since']) ? $configuration['changed_since'] : null
            );
        }
        return $configuration['query'];
    }

    public static function buildQueryForSourceTags(array $tags, $changedSince = null)
    {
        $query = '';
        if ($changedSince && $changedSince !== InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE) {
            $query = '(';
        }
        $query .= implode(
            ' AND ',
            array_map(function (array $tag) {
                $queryPart = sprintf('tags:"%s"', $tag['name']);
                if ($tag['match'] === TagsRewriteHelper::MATCH_TYPE_EXCLUDE) {
                    $queryPart = 'NOT ' . $queryPart;
                }
                return $queryPart;
            }, $tags)
        );
        if ($changedSince && $changedSince !== InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE) {
            $query .= ') AND ' . self::getChangedSinceQueryPortion($changedSince);
        }
        return $query;
    }

    public static function getChangedSinceQueryPortion($changedSince)
    {
        return sprintf(
            'created:>"%s"',
            date('Y-m-d H:i:s', strtotime($changedSince))
        );
    }

    public static function getTagsFromSourceTags(array $tags)
    {
        return array_map(function ($tag) {
            return $tag['name'];
        }, $tags);
    }

    public static function getSourceTagsFromTags(array $tags)
    {
        return array_map(function ($tag) {
            return [
                'name' => $tag
            ];
        }, $tags);
    }
}
