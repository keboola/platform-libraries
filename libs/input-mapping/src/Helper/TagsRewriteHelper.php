<?php

namespace Keboola\InputMapping\Helper;

use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TagsRewriteHelper
{
    public static function rewriteFileTags(
        $fileConfiguration,
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
            $oldTagsList = BuildQueryFromConfigurationHelper::getTagsFromSourceTags(
                $fileConfiguration['source']['tags']
            );
            $newTagsList = self::overwriteTags($prefix, $oldTagsList);

            if (self::hasFilesWithSourceTags($clientWrapper, $newTagsList)) {
                $logger->info(
                    sprintf(
                        'Using dev source tags "%s" instead of "%s".',
                        implode(', ', $newTagsList),
                        implode(', ', $oldTagsList)
                    )
                );

                return array_replace_recursive($fileConfiguration, [
                    'source' => [
                        'tags' => BuildQueryFromConfigurationHelper::getSourceTagsFromTags(
                            $newTagsList
                        ),
                    ],
                ]);
            }
        }

        return $fileConfiguration;
    }

    private static function overwriteTags($prefix, $tags)
    {
        return array_map(function ($tag) use ($prefix) {
            return $prefix . '-' . $tag;
        }, $tags);
    }

    private static function hasFilesWithTags($clientWrapper, $tags)
    {
        $options = new ListFilesOptions();
        $options->setTags($tags);
        $options->setLimit(1);

        return count($clientWrapper->getBasicClient()->listFiles($options)) > 0;
    }

    private static function hasFilesWithSourceTags($clientWrapper, $tags)
    {
        $options = new ListFilesOptions();
        $options->setQuery(BuildQueryFromConfigurationHelper::buildQueryForTags($tags));
        $options->setLimit(1);

        return count($clientWrapper->getBasicClient()->listFiles($options)) > 0;
    }
}
