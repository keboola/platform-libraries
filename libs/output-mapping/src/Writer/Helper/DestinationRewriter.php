<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;

class DestinationRewriter
{
    public static function rewriteDestination(array $config, ClientWrapper $clientWrapper): array
    {
        if (!$clientWrapper->hasBranch()) {
            return $config;
        }

        $tableIdParts = explode('.', $config['destination']);
        if (count($tableIdParts) !== 3) {
            throw new InvalidOutputException(sprintf('Invalid destination: "%s"', $config['destination']));
        }

        $bucketId = $tableIdParts[1];
        $prefix = '';
        if (str_starts_with($bucketId, 'c-')) {
            $bucketId = substr($bucketId, 2);
            $prefix = 'c-';
        }

        try {
            $webalizeResult = $clientWrapper->getBranchClientIfAvailable()->webalizeDisplayName(
                (string) $clientWrapper->getBranchId()
            );
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Cannot upload file to table "%s" in Storage API: %s',
                    $config['destination'],
                    $e->getMessage()
                ),
                $e->getCode(),
                $e
            );
        }

        $bucketId = $webalizeResult['displayName'] . '-' . $bucketId;

        // this assumes that bucket id starts with c-
        // https://github.com/keboola/output-mapping/blob/f6451d2faa825913db2ce986952a9ad6db082e50/src/Writer/TableWriter.php#L498
        $tableIdParts[1] =  $prefix . $bucketId;
        $config['destination'] = implode('.', $tableIdParts);

        return $config;
    }
}
