<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\InputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractTableStrategy implements StrategyInterface
{
    /**
     * @param Adapter::FORMAT_* $format
     */
    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        protected readonly string $format,
        protected readonly bool $isFailedJob = false,
    ) {
    }
}
