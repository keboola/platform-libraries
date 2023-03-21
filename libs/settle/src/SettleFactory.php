<?php

declare(strict_types=1);

namespace Keboola\Settle;

use Psr\Log\LoggerInterface;

class SettleFactory
{
    private LoggerInterface $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function createSettle(int $maxAttempts, int $maxAttemptsDelay): Settle
    {
        return new Settle($this->logger, $maxAttempts, $maxAttemptsDelay);
    }
}
