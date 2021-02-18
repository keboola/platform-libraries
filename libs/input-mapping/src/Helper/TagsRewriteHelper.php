<?php

namespace Keboola\InputMapping\Helper;

use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TagsRewriteHelper
{
    public static function rewriteFileTags(
        array $fileConfiguration,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ) {
        if (!$clientWrapper->hasBranch()) {
            return $fileConfiguration;
        }

        $prefix = $clientWrapper
            ->getBasicClient()
            ->webalizeDisplayName((string) $clientWrapper->getBranchId())['displayName'];

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
            $newTagsList = self::overwriteSourceTags($prefix, $oldTagsList);

            if (self::hasFilesWithSourceTags($clientWrapper, $newTagsList)) {
                $logger->info(
                    sprintf(
                        'Using dev source tags "%s" instead of "%s".',
                        implode(', ', BuildQueryFromConfigurationHelper::getTagsFromSourceTags($newTagsList)),
                        implode(', ', BuildQueryFromConfigurationHelper::getTagsFromSourceTags($oldTagsList))
                    )
                );

                $fileConfiguration['source']['tags'] = $newTagsList;
            }
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
