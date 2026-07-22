<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\StorageApiBranch\Factory\AuthType;
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
    public function __construct(
        private readonly StorageApiTokenFactory $tokenFactory,
    ) {
    }

    public function extractCredential(Request $request): ?RequestToken
    {
        return RequestToken::tryFromRequest($request);
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
                ->exchangeFromProgrammaticToken($request, $credential->token),
            RequestTokenType::OAuthToken => $this->tokenFactory
                ->createFromValue($request, $credential->token, AuthType::BEARER),
            RequestTokenType::StorageToken => $this->tokenFactory
                ->createFromValue($request, $credential->token, AuthType::STORAGE_TOKEN),
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
