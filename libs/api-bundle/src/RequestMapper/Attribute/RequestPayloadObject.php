<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper\Attribute;

use Attribute;
use Keboola\ApiBundle\RequestMapper\PayloadFormat;

#[Attribute(Attribute::TARGET_PARAMETER)]
class RequestPayloadObject implements RequestMapperAttributeInterface
{
    public function __construct(
        public readonly bool $allowExtraKeys = true,
        /**
         * Wire format of the request body. Form payloads are read from the request parameters
         * rather than the raw body, and are mapped with casting enabled because form values
         * are always strings.
         */
        public readonly PayloadFormat $format = PayloadFormat::Json,
    ) {
    }
}
