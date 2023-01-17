<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractWriter
{
    public const SYSTEM_KEY_COMPONENT_ID = 'componentId';
    public const SYSTEM_KEY_CONFIGURATION_ID = 'configurationId';
    public const SYSTEM_KEY_CONFIGURATION_ROW_ID = 'configurationRowId';
    public const SYSTEM_KEY_BRANCH_ID = 'branchId';
    public const SYSTEM_KEY_RUN_ID = 'runId';

    protected ClientWrapper $clientWrapper;
    protected LoggerInterface $logger;

    /**
     * @var Adapter::FORMAT_YAML | Adapter::FORMAT_JSON
     */
    protected string $format = 'json';
    protected StrategyFactory $strategyFactory;

    public function __construct(StrategyFactory $strategyFactory)
    {
        $this->clientWrapper = $strategyFactory->getClientWrapper();
        $this->logger = $strategyFactory->getLogger();
        $this->strategyFactory = $strategyFactory;
    }

    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function setFormat(string $format): void
    {
        $this->format = $format;
    }
}
