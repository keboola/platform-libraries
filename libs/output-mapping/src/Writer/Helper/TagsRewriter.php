<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StorageApiBranch\ClientWrapper;

class TagsRewriter
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

    public static function addSystemTags(array $storageConfig, array $systemMetadata)
    {
        if (!empty($systemMetadata)) {
            foreach ($systemMetadata as $systemKey => $systemValue) {
                switch ($systemKey) {
                    case FileWriter::SYSTEM_KEY_COMPONENT_ID:
                        $storageConfig['tags'][] = FileWriter::SYSTEM_KEY_COMPONENT_ID . ': ' . $systemValue;
                        break;
                    case FileWriter::SYSTEM_KEY_CONFIGURATION_ID:
                        $storageConfig['tags'][] = FileWriter::SYSTEM_KEY_CONFIGURATION_ID . ': ' . $systemValue;
                        break;
                    case FileWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID:
                        $storageConfig['tags'][] = FileWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID . ': ' . $systemValue;
                        break;
                    case FileWriter::SYSTEM_KEY_BRANCH_ID:
                        $storageConfig['tags'][] = FileWriter::SYSTEM_KEY_BRANCH_ID . ': ' . $systemValue;
                        break;
                }
            }
        }
        return $storageConfig;
    }
}
