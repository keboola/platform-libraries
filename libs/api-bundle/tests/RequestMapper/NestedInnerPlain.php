<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

class NestedInnerPlain
{
    public function __construct(
        public readonly string $secret,
    ) {
    }
}
