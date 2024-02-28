<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\StorageApiBranch\ClientWrapper;

class BranchResolver
{
    public function __construct(private readonly ClientWrapper $clientWrapper,)
    {
        
    }
    
    public function rewriteBranchSource(array $config): array
    {
        if (!$this->clientWrapper->getClientOptionsReadOnly()->useBranchStorage()) {
            $config = DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
        }

        return $config;
    }
}