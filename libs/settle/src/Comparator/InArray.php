<?php

declare(strict_types=1);

namespace Keboola\Settle\Comparator;

/**
 * @template TValue
 * @implements ComparatorInterface<TValue>
 */
class InArray implements ComparatorInterface
{
    /**
     * @phpstan-param TValue[] $targetValues
     * @param array $targetValues
     */
    public function __construct(private readonly array $targetValues, private readonly bool $strict)
    {
    }

    /**
     * @inheritDoc
     */
    public function __invoke($currentValue): bool
    {
        return in_array($currentValue, $this->targetValues, $this->strict);
    }
}
