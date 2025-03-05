<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageApiToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\ManageApiTokenAuth;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * @implements TokenAuthenticatorInterface<ManageApiToken>
 */
class ManageApiTokenAuthenticator implements TokenAuthenticatorInterface
{
    public function __construct(
        private readonly ManageApiClientFactory $manageApiClientFactory,
    ) {
    }

    public function getTokenHeader(): string
    {
        return 'X-KBC-ManageApiToken';
    }

    public function authenticateToken(AuthAttributeInterface $authAttribute, string $token): ManageApiToken
    {
        assert($authAttribute instanceof ManageApiTokenAuth);

        $manageApiClient = $this->manageApiClientFactory->getClient($token);

        try {
            $tokenData = $manageApiClient->verifyToken();
        } catch (ManageApiClientException $e) {
            throw new CustomUserMessageAuthenticationException($e->getMessage(), [], 0, $e);
        }

        return ManageApiToken::fromVerifyResponse($tokenData);
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof ManageApiTokenAuth);
        assert($token instanceof ManageApiToken);

        if ($authAttribute->isSuperAdmin === false && $token->isSuperAdmin() === true) {
            throw new AccessDeniedException('Authentication token must not be super admin');
        }

        if ($authAttribute->isSuperAdmin === true && $token->isSuperAdmin() === false) {
            throw new AccessDeniedException('Authentication token is not super admin');
        }

        $missingScopes = array_diff($authAttribute->scopes, $token->getScopes());
        if (count($missingScopes) > 0) {
            throw new AccessDeniedException(sprintf(
                'Authentication token is valid but missing following scopes: %s',
                implode(', ', $missingScopes),
            ));
        }
    }
}
