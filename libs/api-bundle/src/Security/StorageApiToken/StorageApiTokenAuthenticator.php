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
 * directly against Storage API. Connection programmatic tokens (kbc_at_* / kbc_pat_*) are
 * exchanged through Manage API's auth-bridge resolver, which returns the legacy Storage token
 * together with its full detail, so no Storage verification call follows. Both paths are
 * implemented by {@see StorageApiTokenFactory} and yield a {@see StorageApiToken}; this
 * authenticator only classifies the token, picks the path and checks the required features.
 *
 * @implements TokenAuthenticatorInterface<RequestToken, StorageApiToken>
 */
class StorageApiTokenAuthenticator implements TokenAuthenticatorInterface
{
    private const BEARER_PATTERN = '/^Bearer\s+(.+)$/i';
    private const AUTHORIZATION_HEADER = 'Authorization';
    private const TOKEN_HEADER = 'X-StorageApi-Token';

    public function __construct(
        private readonly StorageApiTokenFactory $tokenFactory,
    ) {
    }

    /**
     * Single source of truth for the token an incoming request carries and its {@see RequestTokenType}:
     * `Authorization: Bearer <kbc_at_*|kbc_pat_*>` is {@see RequestTokenType::Programmatic}, any other
     * `Authorization: Bearer <t>` is {@see RequestTokenType::OAuthToken}, and any non-Bearer
     * `Authorization` value (taken verbatim) or the `X-StorageApi-Token` header is
     * {@see RequestTokenType::StorageToken}. Returns null when no token is present.
     */
    public function extractCredential(Request $request): ?RequestToken
    {
        $authHeader = $request->headers->get(self::AUTHORIZATION_HEADER);
        if ($authHeader !== null) {
            if (preg_match(self::BEARER_PATTERN, $authHeader, $matches) === 1) {
                $bearerToken = $matches[1];
                $type = ProgrammaticToken::matches($bearerToken)
                    ? RequestTokenType::Programmatic
                    : RequestTokenType::OAuthToken;

                return new RequestToken($bearerToken, $type);
            }

            // A non-Bearer Authorization value is taken verbatim and treated as a legacy token,
            // preserving the pre-exchange behaviour for that (undocumented) carrier.
            return new RequestToken($authHeader, RequestTokenType::StorageToken);
        }

        $storageToken = $request->headers->get(self::TOKEN_HEADER);
        if ($storageToken !== null && $storageToken !== '') {
            return new RequestToken($storageToken, RequestTokenType::StorageToken);
        }

        return null;
    }

    /**
     * Routes the already-classified credential to the matching {@see StorageApiTokenFactory} method:
     * a programmatic token is exchanged, an OAuth bearer / legacy Storage token is verified. The
     * credential comes straight from {@see self::extractCredential()} — the request is not parsed
     * again here.
     *
     * @param RequestToken $credential
     */
    public function authenticateToken(
        AuthAttributeInterface $authAttribute,
        mixed $credential,
        Request $request,
    ): StorageApiToken {
        assert($authAttribute instanceof StorageApiTokenAuth);

        return match ($credential->type) {
            RequestTokenType::Programmatic => $this->tokenFactory
                ->createFromProgrammaticToken($request, $credential->token),
            RequestTokenType::OAuthToken => $this->tokenFactory
                ->createFromOAuthToken($request, $credential->token),
            RequestTokenType::StorageToken => $this->tokenFactory
                ->createFromStorageToken($request, $credential->token),
        };
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
