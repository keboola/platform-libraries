<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractWriter
{
    const SYSTEM_KEY_COMPONENT_ID = 'componentId';
    const SYSTEM_KEY_CONFIGURATION_ID = 'configurationId';
    const SYSTEM_KEY_CONFIGURATION_ROW_ID = 'configurationRowId';
    const SYSTEM_KEY_BRANCH_ID = 'branchId';

    /**
     * @var ClientWrapper
     */
    protected $clientWrapper;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var string
     */
    protected $format = 'json';

    /**
     * @var StrategyFactory
     */
    protected $strategyFactory;

    public function __construct(StrategyFactory $strategyFactory)
    {
        $this->clientWrapper = $strategyFactory->getClientWrapper();
        $this->logger = $strategyFactory->getLogger();
        $this->strategyFactory = $strategyFactory;
    }

    /**
     * @param string $format
     */
    public function setFormat($format)
    {
        $this->format = $format;
    }
}
