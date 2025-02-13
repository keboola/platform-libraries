<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS | Attribute::TARGET_METHOD | Attribute::IS_REPEATABLE)]
class ManageApiTokenAuth implements AuthAttributeInterface
{
    public function __construct(
        public readonly array $scopes = [],
        public readonly ?bool $isSuperAdmin = null,
        public readonly array $features = [],
    ) {
    }
}
