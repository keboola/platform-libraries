<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use Keboola\ApiBundle\RequestMapper\Attribute\RequestKey;

class NestedInnerWithKey
{
    public function __construct(
        #[RequestKey('#secret')]
        public readonly string $secret,
    ) {
    }
}
