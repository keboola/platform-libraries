<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;

interface StrategyInterface
{
    /**
     * @return ProviderInterface
     */
    public function getDataStorage();

    /**
     * @return ProviderInterface
     */
    public function getMetadataStorage();
}
