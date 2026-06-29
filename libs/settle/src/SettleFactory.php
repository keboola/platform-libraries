<?php

declare(strict_types=1);

namespace Keboola\Settle;

use Psr\Log\LoggerInterface;

class SettleFactory
{
    public function __construct(private readonly LoggerInterface $logger)
    {
    }

    public function createSettle(int $maxAttempts, int $maxAttemptsDelay): Settle
    {
        return new Settle($this->logger, $maxAttempts, $maxAttemptsDelay);
    }
}
