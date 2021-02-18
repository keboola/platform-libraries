<?php

namespace Keboola\InputMapping\Helper;

class BuildQueryFromConfigurationHelper
{
    const MATCH_TYPE_EXCLUDE = 'exclude';

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
            return self::buildQueryForSourceTags($configuration['source']['tags']);
        }
        return $configuration['query'];
    }

    public static function buildQueryForSourceTags(array $tags)
    {
        return implode(
            ' AND ',
            array_map(function (array $tag) {
                $queryPart = sprintf('tags:"%s"', $tag['name']);
                if ($tag['match'] === self::MATCH_TYPE_EXCLUDE) {
                    $queryPart = 'NOT ' . $queryPart;
                }
                return $queryPart;
            }, $tags)
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
