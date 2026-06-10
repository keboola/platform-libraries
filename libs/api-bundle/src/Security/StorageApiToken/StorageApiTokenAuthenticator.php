<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

/**
 * Authenticates a Storage API token. Legacy tokens (X-StorageApi-Token / Bearer) are verified
 * directly against Storage API. Connection programmatic tokens (kbc_at_* / kbc_pat_*) are first
 * exchanged for a legacy Storage token through Manage API's auth-bridge resolver, then verified
 * the same way. Both paths are implemented by {@see StorageApiTokenFactory} and yield a
 * {@see StorageApiToken}; this authenticator only extracts the token, picks the path and checks
 * the required features.
 *
 * @implements TokenAuthenticatorInterface<StorageApiToken>
 */
class StorageApiTokenAuthenticator implements TokenAuthenticatorInterface
{
    private const BEARER_PATTERN = '/^Bearer\s+(.+)$/i';

    public function __construct(
        private readonly StorageApiTokenFactory $tokenFactory,
    ) {
    }

    public function extractToken(Request $request): ?string
    {
        // Check Authorization header first
        $authHeader = $request->headers->get('Authorization');
        if ($authHeader !== null) {
            // Validate it's a Bearer token and strip prefix
            if (preg_match(self::BEARER_PATTERN, $authHeader, $matches)) {
                return $matches[1];
            }
            return $authHeader;
        }

        // Check X-StorageApi-Token header
        return $request->headers->get('X-StorageApi-Token');
    }

    public function authenticateToken(
        AuthAttributeInterface $authAttribute,
        string $token,
        Request $request,
    ): StorageApiToken {
        assert($authAttribute instanceof StorageApiTokenAuth);

        // Exchange is restricted to programmatic tokens that explicitly arrive as
        // `Authorization: Bearer <kbc_at|kbc_pat>`. A bare `Authorization: <kbc_...>` or
        // `X-StorageApi-Token: kbc_...` is an undocumented shape and stays on the legacy
        // verification path, preserving pre-exchange behaviour.
        $programmaticToken = $this->extractProgrammaticBearerToken($request);
        if ($programmaticToken !== null) {
            return $this->tokenFactory->createFromProgrammaticToken($request, $programmaticToken);
        }

        return $this->tokenFactory->createFromRequest($request);
    }

    /**
     * Returns the programmatic token only when it is presented as `Authorization: Bearer <kbc_...>`,
     * otherwise null. Other carriers (bare Authorization value, X-StorageApi-Token) are not eligible
     * for exchange.
     */
    private function extractProgrammaticBearerToken(Request $request): ?string
    {
        $authHeader = $request->headers->get('Authorization');
        if ($authHeader === null || !preg_match(self::BEARER_PATTERN, $authHeader, $matches)) {
            return null;
        }

        $bearerToken = $matches[1];

        return ProgrammaticToken::matches($bearerToken) ? $bearerToken : null;
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof StorageApiTokenAuth);
        assert($token instanceof StorageApiToken);

        $missingFeatures = array_diff($authAttribute->features, $token->getFeatures());
        if (count($missingFeatures) > 0) {
            throw new AccessDeniedException(sprintf(
                'Authentication token is valid but missing following features: %s',
                implode(', ', $missingFeatures),
            ));
        }
    }
}
