<?php

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\StagingProvider\Staging\StagingInterface;

interface StagingProviderInterface extends ProviderInterface
{
    /**
     * @return StagingInterface
     */
    public function getStaging();
}
