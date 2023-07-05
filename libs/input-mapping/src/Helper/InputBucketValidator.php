<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;

class InputBucketValidator
{
    public static function checkDevBuckets(InputTableOptionsList $tablesDefinitions, ClientWrapper $clientWrapper): void
    {
        $buckets = [];
        foreach ($tablesDefinitions->getTables() as $tablesDefinition) {
            $bucketId = self::getBucketIdFromSource($tablesDefinition->getSource());
            if (!isset($buckets[$bucketId])) {
                $buckets[$bucketId] = self::isDevBucket($bucketId, $clientWrapper);
            }
        }

        $bucketIds = array_keys(array_filter($buckets));
        if ($bucketIds) {
            throw new InvalidInputException(sprintf(
                'The buckets "%s" come from a development branch ' .
                    'and must not be used directly in input mapping.',
                implode(', ', $bucketIds)
            ));
        }
    }

    private static function getBucketIdFromSource(string $source): string
    {
        $parts = explode('.', $source);
        if (count($parts) < 3) {
            return '';
        }
        return $parts[0] . '.' . $parts[1];
    }

    private static function isDevBucket(string $bucketId, ClientWrapper $clientWrapper): bool
    {
        try {
            $metadata = new Metadata($clientWrapper->getTableAndFileStorageClient());
            $metadata = $metadata->listBucketMetadata($bucketId);
            foreach ($metadata as $metadatum) {
                if (($metadatum['key'] === 'KBC.lastUpdatedBy.branch.id') ||
                    ($metadatum['key'] === 'KBC.createdBy.branch.id')) {
                    return true;
                }
            }
        } catch (ClientException $e) {
            /* If the bucket does not exist it's "ok" in that it is not a DEV bucket. It will fail later though,
                but that's none of our business here. */
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        return false;
    }
}
