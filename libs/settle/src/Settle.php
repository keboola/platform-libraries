<?php

declare(strict_types=1);

namespace Keboola\Settle;

use Psr\Log\LoggerInterface;
use RuntimeException;

class Settle
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly int $maxAttempts,
        private readonly int $maxAttemptsDelay,
    ) {
    }

    /**
     * @param callable(TValue $currentValue): bool $comparator
     * @param callable(): TValue $getCurrentValue
     * @return mixed
     * @phpstan-return TValue
     * @template TValue
     */
    public function settle(callable $comparator, callable $getCurrentValue)
    {
        $attempt = 1;
        while (true) {
            $this->logger->debug('Checking current value', [
                'attempt' => $attempt,
            ]);

            $currentValue = $getCurrentValue();
            $valueMatches = $comparator($currentValue);

            if ($valueMatches) {
                $this->logger->debug('Condition settled', [
                    'currentValue' => $this->printValue($currentValue),
                    'attempts' => $attempt,
                ]);

                return $currentValue;
            }

            $this->logger->debug('Current value does not match expectation', [
                'currentValue' => $this->printValue($currentValue),
                'attempts' => $attempt,
            ]);

            if ($attempt >= $this->maxAttempts) {
                throw new RuntimeException(sprintf(
                    'Failed to settle condition, actual value "%s" does not match expectation',
                    $this->printValue($currentValue),
                ));
            }

            sleep((int) min(2 ** $attempt, $this->maxAttemptsDelay));
            $attempt++;
        }
    }

    /**
     * @param mixed $value
     */
    private function printValue($value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR);
    }
}
