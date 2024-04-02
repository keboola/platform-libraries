<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use Attribute;
use Keboola\ApiBundle\RequestMapper\Attribute\RequestMapperAttributeInterface;

#[Attribute(Attribute::TARGET_PARAMETER)]
class UnsupportedMapperAttribute implements RequestMapperAttributeInterface
{

}
