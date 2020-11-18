<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApiBranch\ClientWrapper;

class DestinationRewriter
{
    public static function rewriteDestination(array $config, ClientWrapper $clientWrapper)
    {
        if ($clientWrapper->hasBranch()) {
            $tableIdParts = explode('.', $config['destination']);
            if (count($tableIdParts) !== 3) {
                throw new InvalidOutputException(sprintf('Invalid destination: "%s"', $config['destination']));
            }
            $bucketId = $tableIdParts[1];
            if (substr($bucketId, 0, 2) === 'c-') {
                $bucketId = substr($bucketId, 2);
            }
            $bucketId = $clientWrapper->getBranch() . '-' . $bucketId;
            // this assumes that bucket id starts with c-
            // https://github.com/keboola/output-mapping/blob/f6451d2faa825913db2ce986952a9ad6db082e50/src/Writer/TableWriter.php#L498
            $tableIdParts[1] = 'c-' . $bucketId;
            $config['destination'] = implode('.', $tableIdParts);
        }
        return $config;
    }
}
