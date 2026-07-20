<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use SensitiveParameter;

/**
 * A token extracted from an incoming request together with its classified {@see RequestTokenType},
 * the single source of truth for how the token is subsequently handled.
 */
final class RequestToken
{
    public function __construct(
        #[SensitiveParameter]
        public readonly string $token,
        public readonly RequestTokenType $type,
    ) {
    }
}
