<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use Keboola\ApiBundle\RequestMapper\Attribute\RequestKey;

class DuplicateKeyRequestData
{
    public function __construct(
        #[RequestKey('#data')]
        public readonly string $first,
        #[RequestKey('#data')]
        public readonly string $second,
    ) {
    }
}
