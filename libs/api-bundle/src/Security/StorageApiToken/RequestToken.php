<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;

/**
 * A token extracted from an incoming request together with its classified {@see RequestTokenType},
 * the single source of truth for how the token is subsequently handled.
 */
final class RequestToken
{
    private const BEARER_PATTERN = '/^Bearer\s+(.+)$/i';
    private const AUTHORIZATION_HEADER = 'Authorization';
    private const TOKEN_HEADER = 'X-StorageApi-Token';

    /** Prefixes of Connection programmatic bearer tokens: kbc_at_* access, kbc_pat_* personal. */
    private const PROGRAMMATIC_TOKEN_PREFIXES = ['kbc_at_', 'kbc_pat_'];

    public function __construct(
        #[SensitiveParameter]
        public readonly string $token,
        public readonly RequestTokenType $type,
    ) {
    }

    /**
     * Classify the token an incoming request carries, or null if none is present (named after
     * {@see \BackedEnum::tryFrom()}: absence of a token is an expected outcome, not an error):
     * `Authorization: Bearer <kbc_at_*|kbc_pat_*>` is {@see RequestTokenType::Programmatic}, any other
     * `Authorization: Bearer <t>` is {@see RequestTokenType::OAuthToken}, and any non-Bearer
     * `Authorization` value (taken verbatim) or the `X-StorageApi-Token` header is
     * {@see RequestTokenType::StorageToken}. A present-but-empty header value counts as absent (so
     * an empty `Authorization` does not shadow a non-empty `X-StorageApi-Token`), and an empty token
     * yields null rather than a doomed empty-token verification call. The `Authorization` header
     * takes precedence over `X-StorageApi-Token`.
     */
    public static function tryFromRequest(Request $request): ?self
    {
        $authHeader = $request->headers->get(self::AUTHORIZATION_HEADER);
        if ($authHeader !== null && $authHeader !== '') {
            if (preg_match(self::BEARER_PATTERN, $authHeader, $matches) === 1) {
                $bearerToken = $matches[1];
                $type = self::isProgrammatic($bearerToken)
                    ? RequestTokenType::Programmatic
                    : RequestTokenType::OAuthToken;

                return new self($bearerToken, $type);
            }

            // A non-Bearer Authorization value is taken verbatim and treated as a legacy token,
            // preserving the pre-exchange behaviour for that (undocumented) carrier.
            return new self($authHeader, RequestTokenType::StorageToken);
        }

        $storageToken = $request->headers->get(self::TOKEN_HEADER);
        if ($storageToken !== null && $storageToken !== '') {
            return new self($storageToken, RequestTokenType::StorageToken);
        }

        return null;
    }

    private static function isProgrammatic(string $token): bool
    {
        foreach (self::PROGRAMMATIC_TOKEN_PREFIXES as $prefix) {
            if (str_starts_with($token, $prefix)) {
                return true;
            }
        }

        return false;
    }
}
