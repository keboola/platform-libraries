<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Attribute;

use Attribute;

/**
 * Authenticates Connection programmatic tokens (kbc_at_* / kbc_pat_*) sent as
 * `Authorization: Bearer ...` together with an `X-KBC-ProjectId` header. The programmatic token is
 * exchanged for a legacy Storage token, yielding a StorageApiToken (same as #[StorageApiTokenAuth]).
 */
#[Attribute(Attribute::TARGET_CLASS | Attribute::TARGET_METHOD | Attribute::IS_REPEATABLE)]
class ConnectionTokenAuth implements AuthAttributeInterface
{
    public function __construct(
        /** @var list<string> */ public readonly array $features = [],
    ) {
    }
}
