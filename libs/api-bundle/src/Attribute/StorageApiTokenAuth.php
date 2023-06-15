<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS | Attribute::TARGET_METHOD | Attribute::IS_REPEATABLE)]
class StorageApiTokenAuth implements AuthAttributeInterface
{
    public function __construct(
        public readonly array $features,
    ) {
    }
}
