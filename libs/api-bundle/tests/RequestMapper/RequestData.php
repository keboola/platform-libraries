<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use Symfony\Component\Validator\Constraints as Assert;

class RequestData
{
    public function __construct(
        #[Assert\Length(min: 1, max: 16)]
        public readonly string $name,
    ) {
    }
}
