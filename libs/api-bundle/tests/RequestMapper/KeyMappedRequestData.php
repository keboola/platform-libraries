<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use Keboola\ApiBundle\RequestMapper\Attribute\RequestKey;
use Symfony\Component\Validator\Constraints as Assert;

class KeyMappedRequestData
{
    public function __construct(
        #[RequestKey('#data')]
        #[Assert\Length(max: 16)]
        public readonly string $data,
        public readonly ?string $name = null,
    ) {
    }
}
