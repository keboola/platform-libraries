<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractWriter
{
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
