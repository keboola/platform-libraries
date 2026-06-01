<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Attribute;

use Attribute;

/**
 * Authenticates the request against Connection using either a Manage API token
 * (`X-KBC-ManageApiToken`) or a Kubernetes ServiceAccount JWT
 * (`X-Kubernetes-Authorization: Bearer <jwt>`). Connection resolves both to a
 * Manage token (synthetic for the ServiceAccount case), so `scopes` and
 * `isSuperAdmin` are checked identically regardless of the header used.
 */
#[Attribute(Attribute::TARGET_CLASS | Attribute::TARGET_METHOD | Attribute::IS_REPEATABLE)]
class ApplicationTokenAuth implements AuthAttributeInterface
{
    public function __construct(
        /** @var list<string> */ public readonly array $scopes = [],
        public readonly ?bool $isSuperAdmin = null,
    ) {
    }
}
