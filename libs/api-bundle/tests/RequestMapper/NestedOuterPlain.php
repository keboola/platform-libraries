<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

class NestedOuterPlain
{
    public function __construct(
        public readonly string $name,
        public readonly NestedInnerPlain $inner,
        public readonly int|string $union = 0,
    ) {
    }
}
