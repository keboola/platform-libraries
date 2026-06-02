<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ConnectionToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\ConnectionTokenAuth;
use Keboola\ApiBundle\AuthBridge\ProgrammaticToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenExchange;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

/**
 * @implements TokenAuthenticatorInterface<StorageApiToken>
 */
class ConnectionTokenAuthenticator implements TokenAuthenticatorInterface
{
    public function __construct(
        private readonly StorageApiTokenExchange $tokenExchange,
        private readonly string $projectIdHeader = 'X-KBC-ProjectId',
    ) {
    }

    public function extractToken(Request $request): ?string
    {
        $authHeader = $request->headers->get('Authorization');
        if ($authHeader === null) {
            return null;
        }

        if (preg_match('/^Bearer\s+(.+)$/i', $authHeader, $matches)) {
            $token = $matches[1];
        } else {
            $token = $authHeader;
        }

        // Only handle programmatic tokens; leave anything else for other authenticators.
        if (!ProgrammaticToken::matches($token)) {
            return null;
        }

        return $token;
    }

    public function authenticateToken(
        AuthAttributeInterface $authAttribute,
        string $token,
        Request $request,
    ): StorageApiToken {
        assert($authAttribute instanceof ConnectionTokenAuth);

        return $this->tokenExchange->exchange($request, $token, $this->projectIdHeader);
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof ConnectionTokenAuth);
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
