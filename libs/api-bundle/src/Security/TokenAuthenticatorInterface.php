<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Symfony\Component\Security\Core\Exception\AuthenticationException;

/**
 * @template TokenType of TokenInterface
 */
interface TokenAuthenticatorInterface
{
    public function getTokenHeader(): string;

    /**
     * @return TokenType
     * @throws AuthenticationException
     */
    public function authenticateToken(AuthAttributeInterface $authAttribute, string $token): TokenInterface;

    /**
     * @param TokenType $token
     */
    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void;
}
