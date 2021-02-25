<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TagsHelper
{
    public static function rewriteTags(array $storageConfig, ClientWrapper $clientWrapper)
    {
        if (!empty($storageConfig['tags']) && $clientWrapper->hasBranch()) {
            $prefix = $clientWrapper->getBasicClient()->webalizeDisplayName((string) $clientWrapper->getBranchId())['displayName'];
            $storageConfig['tags'] = array_map(function ($tag) use ($prefix) {
                return $prefix . '-' . $tag;
            }, $storageConfig['tags']);
        }

        return $storageConfig;
    }

    public static function addSystemTags(array $storageConfig, array $systemMetadata, LoggerInterface $logger)
    {
        if (!empty($systemMetadata)) {
            foreach ($systemMetadata as $systemKey => $systemValue) {
                if (in_array($systemKey, [
                    FileWriter::SYSTEM_KEY_COMPONENT_ID,
                    FileWriter::SYSTEM_KEY_CONFIGURATION_ID,
                    FileWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID,
                    FileWriter::SYSTEM_KEY_BRANCH_ID,
                ])) {
                    $storageConfig['tags'][] = $systemKey . ': ' . $systemValue;
                } else {
                    $logger->info(sprintf('Not generating tag for key: %s', $systemKey));
                }
            }
        }
        return $storageConfig;
    }
}
