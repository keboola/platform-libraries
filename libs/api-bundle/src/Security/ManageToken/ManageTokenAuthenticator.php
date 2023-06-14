<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\ManageTokenAuth;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * @implements TokenAuthenticatorInterface<ManageToken>
 */
class ManageTokenAuthenticator implements TokenAuthenticatorInterface
{
    public function __construct(
        private readonly ManageApiClientFactory $manageApiClientFactory,
    ) {
    }

    public function getTokenHeader(): string
    {
        return 'X-KBC-ManageApiToken';
    }

    public function authenticateToken(AuthAttributeInterface $authAttribute, string $token): ManageToken
    {
        assert($authAttribute instanceof ManageTokenAuth);

        $manageApiClient = $this->manageApiClientFactory->getClient($token);

        try {
            $tokenData = $manageApiClient->verifyToken();
        } catch (ManageApiClientException $e) {
            throw new CustomUserMessageAuthenticationException($e->getMessage(), [], 0, $e);
        }

        return ManageToken::fromVerifyResponse($tokenData);
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof ManageTokenAuth);
        assert($token instanceof ManageToken);

        $missingScopes = array_diff($authAttribute->scopes, $token->getScopes());
        if (count($missingScopes) > 0) {
            throw new AccessDeniedException(sprintf(
                'Authentication token is valid but missing following scopes: %s',
                implode(', ', $missingScopes)
            ));
        }
    }
}
