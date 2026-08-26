<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

/**
 * Monotonic elapsed-time measurement for the phases logged by the input mapping strategies.
 *
 * hrtime() is used instead of microtime() so that an NTP step cannot corrupt a measured duration.
 * The nanosecond counter is kept as a float; at the precision this class reports (hundredths of a
 * second) the mantissa is never the limiting factor.
 */
final class Timer
{
    private function __construct(private readonly float $startedAtNanoseconds)
    {
    }

    public static function start(): self
    {
        return new self((float) hrtime(true));
    }

    public function getElapsedSeconds(): float
    {
        return ((float) hrtime(true) - $this->startedAtNanoseconds) / 1000000000;
    }
}
