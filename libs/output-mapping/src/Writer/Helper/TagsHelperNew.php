<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TagsHelperNew
{
    public static function rewriteTags(array $storageConfig, ClientWrapper $clientWrapper): array
    {
        if (!empty($storageConfig['tags']) && $clientWrapper->isDevelopmentBranch()) {
            $prefix = (string) $clientWrapper->getBranchId();
            $storageConfig['tags'] = array_map(function ($tag) use ($prefix) {
                return $prefix . '-' . $tag;
            }, $storageConfig['tags']);
        }

        return $storageConfig;
    }

    public static function addSystemTags(array $storageConfig, SystemMetadata $systemMetadata): array
    {
        foreach ($systemMetadata->getSystemTags() as $systemKey => $systemValue) {
            $storageConfig['tags'][] = $systemKey . ': ' . $systemValue;
        }
        return $storageConfig;
    }
}
