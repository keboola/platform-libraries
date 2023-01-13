<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\StagingProvider\Staging\StagingInterface;

/**
 * @template T of StagingInterface
 */
interface StagingProviderInterface extends ProviderInterface
{
    /**
     * @return T
     */
    public function getStaging();
}
