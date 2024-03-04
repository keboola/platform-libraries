<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApiBranch\ClientWrapper;

class BranchResolver
{
    public function __construct(private readonly ClientWrapper $clientWrapper,)
    {
        
    }
    
    public function rewriteBranchSource(array $config): array
    {
        if (!$this->clientWrapper->getClientOptionsReadOnly()->useBranchStorage() && $this->clientWrapper->isDevelopmentBranch()) {

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

            $bucketId = $this->clientWrapper->getBranchId() . '-' . $bucketId;

            // this assumes that bucket id starts with c-
            // https://github.com/keboola/output-mapping/blob/f6451d2faa825913db2ce986952a9ad6db082e50/src/Writer/TableWriter.php#L498
            $tableIdParts[1] =  $prefix . $bucketId;
            $config['destination'] = implode('.', $tableIdParts);
        }

        return $config;
    }
}