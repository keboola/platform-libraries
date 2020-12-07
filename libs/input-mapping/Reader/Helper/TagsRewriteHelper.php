<?php

namespace Keboola\InputMapping\Reader\Helper;

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
        if (!empty($fileConfiguration['tags']) && $clientWrapper->hasBranch()) {
            $prefix = $clientWrapper->getBasicClient()->webalizeDisplayName((string) $clientWrapper->getBranchId())['displayName'];

            $newTags = array_map(function ($tag) use ($prefix) {
                return $prefix . '-' . $tag;
            }, $fileConfiguration['tags']);

            $options = new ListFilesOptions();
            $options->setTags($newTags);
            $options->setLimit(1);

            if ($clientWrapper->getBasicClient()->listFiles($options)) {
                $logger->info(
                    sprintf(
                        'Using dev tags "%s" instead of "%s".',
                        implode(', ', $newTags),
                        implode(', ', $fileConfiguration['tags'])
                    )
                );

                $fileConfiguration['tags'] = $newTags;
            }
        }

        return $fileConfiguration;
    }
}
