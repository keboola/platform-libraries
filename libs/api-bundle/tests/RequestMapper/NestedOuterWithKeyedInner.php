<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

class NestedOuterWithKeyedInner
{
    public function __construct(
        public readonly string $name,
        public readonly ?NestedInnerWithKey $inner = null,
    ) {
    }
}
