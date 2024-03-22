<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_PARAMETER)]
class RequestPayloadObject implements RequestMapperAttributeInterface
{
    public function __construct(
        public readonly bool $allowExtraKeys = true,
    ) {
    }
}
