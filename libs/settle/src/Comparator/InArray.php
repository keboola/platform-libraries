<?php

declare(strict_types=1);

namespace Keboola\Settle\Comparator;

/**
 * @template TValue
 * @implements ComparatorInterface<TValue>
 */
class InArray implements ComparatorInterface
{
    private array $targetValues;
    private bool $strict;

    /**
     * @phpstan-param TValue[] $targetValues
     * @param mixed $targetValues
     */
    public function __construct(array $targetValues, bool $strict)
    {
        $this->targetValues = $targetValues;
        $this->strict = $strict;
    }

    /**
     * @inheritDoc
     */
    public function __invoke($currentValue): bool
    {
        return in_array($currentValue, $this->targetValues, $this->strict);
    }
}
