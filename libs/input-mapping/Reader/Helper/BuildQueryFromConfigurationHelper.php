<?php

namespace Keboola\InputMapping\Reader\Helper;

class BuildQueryFromConfigurationHelper
{
    public static function buildQuery($configuration)
    {
        if (isset($configuration['query']) && isset($configuration['source']['tags'])) {
            return sprintf(
                '%s AND (%s)',
                $configuration['query'],
                self::buildQueryForTags(
                    self::getTagsFromSourceTags($configuration['source']['tags'])
                )
            );
        }
        if (isset($configuration['source']['tags'])) {
            return self::buildQueryForTags(
                self::getTagsFromSourceTags($configuration['source']['tags'])
            );
        }
        return $configuration['query'];
    }

    public static function buildQueryForTags($tags)
    {
        return implode(
            ' AND ',
            array_map(function ($tag) {
                return sprintf('tags:"%s"', $tag);
            }, $tags)
        );
    }

    public static function getTagsFromSourceTags($tags)
    {
        return array_map(function ($tag) {
            return $tag['name'];
        }, $tags);
    }

    public static function getSourceTagsFromTags($tags)
    {
        return array_map(function ($tag) {
            return [
                'name' => $tag
            ];
        }, $tags);
    }
}
