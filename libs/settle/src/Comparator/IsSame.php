<?php

declare(strict_types=1);

namespace Keboola\Settle\Comparator;

/**
 * @template TValue
 * @implements ComparatorInterface<TValue>
 */
class IsSame implements ComparatorInterface
{
    /**
     * @phpstan-param TValue $targetValue
     * @param mixed $targetValue
     */
    public function __construct(private readonly mixed $targetValue)
    {
    }

    /**
     * @inheritDoc
     */
    public function __invoke($currentValue): bool
    {
        return $currentValue === $this->targetValue;
    }
}
