<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AuthenticationException;

/**
 * @template TCredential
 * @template TokenType of TokenInterface
 */
interface TokenAuthenticatorInterface
{
    /**
     * Extract the credential the request carries (the shape is authenticator-specific), or null if
     * none is present. Called once per request; its result is handed straight to
     * {@see self::authenticateToken()}.
     *
     * @return TCredential|null
     */
    public function extractCredential(Request $request): mixed;

    /**
     * @param TCredential $credential
     * @return TokenType
     * @throws AuthenticationException
     */
    public function authenticateToken(
        AuthAttributeInterface $authAttribute,
        mixed $credential,
        Request $request,
    ): TokenInterface;

    /**
     * @param TokenType $token
     */
    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void;
}
