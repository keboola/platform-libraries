<?php

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Table\Options\InputTableOptions;

class BuildQueryFromConfigurationHelper
{
    public static function buildQuery($configuration)
    {
        $query = $configuration['query'];
        if (isset($configuration['query']) && isset($configuration['source']['tags'])) {
            $query = sprintf(
                '%s AND (%s)',
                $configuration['query'],
                self::buildQueryForSourceTags($configuration['source']['tags'])
            );
        } elseif (isset($configuration['source']['tags'])) {
            $query = self::buildQueryForSourceTags($configuration['source']['tags']);
        }
        if (isset($configuration['changed_since'])
            && $configuration['changed_since'] !== InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE) {
            $query .= sprintf(
                ' AND created:>%s',
                date('Y-m-d H:i:s', strtotime($configuration['changed_since']))
            );
        }
        return $query;
    }

    public static function buildQueryForSourceTags(array $tags)
    {
        return implode(
            ' AND ',
            array_map(function (array $tag) {
                $queryPart = sprintf('tags:"%s"', $tag['name']);
                if ($tag['match'] === TagsRewriteHelper::MATCH_TYPE_EXCLUDE) {
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
