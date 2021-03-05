<?php

namespace Keboola\InputMapping\Helper;

use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TagsRewriteHelper
{
    const MATCH_TYPE_EXCLUDE = 'exclude';
    const MATCH_TYPE_INCLUDE = 'include';

    public static function rewriteFileTags(
        array $fileConfiguration,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ) {
        if (!$clientWrapper->hasBranch()) {
            return $fileConfiguration;
        }

        $prefix = (string) $clientWrapper->getBranchId();

        if (!empty($fileConfiguration['tags'])) {
            $oldTagsList = $fileConfiguration['tags'];
            $newTagsList = self::overwriteTags($prefix, $oldTagsList);
            if (self::hasFilesWithTags($clientWrapper, $newTagsList)) {
                $logger->info(
                    sprintf(
                        'Using dev tags "%s" instead of "%s".',
                        implode(', ', $newTagsList),
                        implode(', ', $oldTagsList)
                    )
                );
                return array_replace($fileConfiguration, [
                    'tags' => $newTagsList,
                ]);
            }
        }

        if (!empty($fileConfiguration['source']['tags'])) {
            $oldTagsList = $fileConfiguration['source']['tags'];
            $includeTags = array_filter($oldTagsList, function ($tag) {
                return $tag['match'] === self::MATCH_TYPE_INCLUDE;
            });
            $excludeTags = array_filter($oldTagsList, function ($tag) {
                return $tag['match'] === self::MATCH_TYPE_EXCLUDE;
            });
            $newIncludeTags = self::overwriteSourceTags($prefix, $includeTags);

            // here prefix NOT tags only if they are in processed_tags
            $processedTags = isset($fileConfiguration['processed_tags']) ? $fileConfiguration['processed_tags'] : [];

            if (!empty($processedTags)) {
                $processedExcludeTags = array_filter($excludeTags, function ($tag) use ($processedTags) {
                   return in_array($tag['name'], $processedTags);
                });
                $newProcessedExcludeTags = self::overwriteSourceTags($prefix, $processedExcludeTags);

                $newExcludeTags = array_merge(
                    $newProcessedExcludeTags,
                    array_filter($excludeTags, function ($tag) use ($processedTags) {
                        return !in_array($tag['name'], $processedTags);
                    })
                );
                $excludeTags = $newExcludeTags;
            }

            if (self::hasFilesWithSourceTags($clientWrapper, $includeTags)) {
                $logger->info(
                    sprintf(
                        'Using dev source tags "%s" instead of "%s".',
                        implode(', ', BuildQueryFromConfigurationHelper::getTagsFromSourceTags($newIncludeTags)),
                        implode(', ', BuildQueryFromConfigurationHelper::getTagsFromSourceTags($includeTags))
                    )
                );
                $includeTags = $newIncludeTags;
            }
            $fileConfiguration['source']['tags'] = array_merge($includeTags, $excludeTags);
        }
        return $fileConfiguration;
    }

    private static function overwriteTags($prefix, array $tags)
    {
        return array_map(function ($tag) use ($prefix) {
            return $prefix . '-' . $tag;
        }, $tags);
    }

    private static function hasFilesWithTags($clientWrapper, array $tags)
    {
        $options = new ListFilesOptions();
        $options->setTags($tags);
        $options->setLimit(1);

        return count($clientWrapper->getBasicClient()->listFiles($options)) > 0;
    }

    private static function overwriteSourceTags($prefix, array $tags)
    {
        return array_map(function (array $tag) use ($prefix) {
            $tag['name'] = $prefix . '-' . $tag['name'];
            return $tag;
        }, $tags);
    }

    private static function hasFilesWithSourceTags($clientWrapper, array $tags)
    {
        $options = new ListFilesOptions();
        $options->setQuery(BuildQueryFromConfigurationHelper::buildQueryForSourceTags($tags));
        $options->setLimit(1);

        return count($clientWrapper->getBasicClient()->listFiles($options)) > 0;
    }
}
