<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\KubernetesServiceAccount;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\KubernetesServiceAccountAuth;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * @implements TokenAuthenticatorInterface<KubernetesServiceAccountToken>
 */
class KubernetesServiceAccountAuthenticator implements TokenAuthenticatorInterface
{
    public const MANAGE_TOKEN_HEADER = 'X-KBC-ManageApiToken';
    public const SERVICE_ACCOUNT_HEADER = 'X-Kubernetes-Authorization';

    public function __construct(
        private readonly ManageApiClientFactory $manageApiClientFactory,
    ) {
    }

    public function extractToken(Request $request): ?string
    {
        $manageToken = $request->headers->get(self::MANAGE_TOKEN_HEADER);
        if ($manageToken !== null) {
            return $manageToken;
        }

        $serviceAccountHeader = $request->headers->get(self::SERVICE_ACCOUNT_HEADER);
        if ($serviceAccountHeader !== null) {
            if (preg_match('/^Bearer\s+(.+)$/i', $serviceAccountHeader, $matches)) {
                return $matches[1];
            }
            return $serviceAccountHeader;
        }

        return null;
    }

    public function authenticateToken(
        AuthAttributeInterface $authAttribute,
        string $token,
        Request $request,
    ): KubernetesServiceAccountToken {
        assert($authAttribute instanceof KubernetesServiceAccountAuth);

        $manageApiClient = $request->headers->has(self::SERVICE_ACCOUNT_HEADER)
            ? $this->manageApiClientFactory->getClientForJwt($token)
            : $this->manageApiClientFactory->getClient($token);

        try {
            $tokenData = $manageApiClient->verifyToken();
        } catch (ManageApiClientException $e) {
            throw new CustomUserMessageAuthenticationException($e->getMessage(), [], 0, $e);
        }

        return KubernetesServiceAccountToken::fromVerifyResponse($tokenData);
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof KubernetesServiceAccountAuth);
        assert($token instanceof KubernetesServiceAccountToken);

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
