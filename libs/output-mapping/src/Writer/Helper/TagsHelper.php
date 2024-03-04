<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TagsHelper
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

    public static function addSystemTags(array $storageConfig, SystemMetadata $systemMetadata, LoggerInterface $logger): array
    {
        // TODO should be moved to SystemMetadata class as "getSystemTags()"
        foreach ($systemMetadata->asArray() as $systemKey => $systemValue) {
            if (in_array($systemKey, [
                AbstractWriter::SYSTEM_KEY_COMPONENT_ID,
                AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID,
                AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID,
                AbstractWriter::SYSTEM_KEY_BRANCH_ID,
                AbstractWriter::SYSTEM_KEY_RUN_ID,
            ])) {
                $storageConfig['tags'][] = $systemKey . ': ' . $systemValue;
            } else {
                $logger->info(sprintf('Not generating tag for key: %s', $systemKey));
            }
        }
        return $storageConfig;
    }
}
