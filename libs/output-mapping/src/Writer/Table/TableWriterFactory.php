<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Staging\StrategyFactory;

class TableWriterFactory
{
    const FEATURE_NEW_OUTPUT_MAPPING = 'new-table-output-mapping';

    /** @var StrategyFactory */
    private $strategyFactory;

    public function __construct(StrategyFactory $strategyFactory)
    {
        $this->strategyFactory = $strategyFactory;
    }

    public function createTableWriter()
    {
        if ($this->hasNewTableOutputMappingFeature()) {
            return new TableWriterV2($this->strategyFactory);
        }

        return new TableWriterV1($this->strategyFactory);
    }

    private function hasNewTableOutputMappingFeature()
    {
        $token = $this->strategyFactory->getClientWrapper()->getBasicClient()->verifyToken();
        return in_array(self::FEATURE_NEW_OUTPUT_MAPPING, $token['owner']['features'], true);
    }
}
