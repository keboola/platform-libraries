<?php

declare(strict_types=1);

namespace Keboola\Settle\Comparator;

/**
 * @template TValue
 */
interface ComparatorInterface
{
    /**
     * @phpstan-param TValue $currentValue
     * @param mixed $currentValue
     */
    public function __invoke($currentValue): bool;
}
