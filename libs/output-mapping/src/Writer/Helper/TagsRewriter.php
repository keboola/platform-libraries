<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\StorageApiBranch\ClientWrapper;

class TagsRewriter
{
    public static function rewriteTags(array $storageConfig, ClientWrapper $clientWrapper)
    {
        if (!empty($storageConfig['tags']) && $clientWrapper->hasBranch()) {
            $prefix = $clientWrapper->getBasicClient()->webalizeDisplayName($clientWrapper->getBranchName())['displayName'];
            $storageConfig['tags'] = array_map(function ($tag) use ($prefix) {
                return $prefix . '-' . $tag;
            }, $storageConfig['tags']);
        }

        return $storageConfig;
    }
}
