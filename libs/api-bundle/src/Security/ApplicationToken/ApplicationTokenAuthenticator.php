<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ApplicationToken;

use Keboola\ApiBundle\Attribute\ApplicationTokenAuth;
use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * @implements TokenAuthenticatorInterface<ApplicationToken>
 */
class ApplicationTokenAuthenticator implements TokenAuthenticatorInterface
{
    public const MANAGE_TOKEN_HEADER = 'X-KBC-ManageApiToken';
    public const SERVICE_ACCOUNT_HEADER = 'X-Kubernetes-Authorization';

    public function __construct(
        private readonly ManageApiClientFactory $manageApiClientFactory,
    ) {
    }

    public function extractToken(Request $request): ?string
    {
        // Returned verbatim (the ServiceAccount header keeps its "Bearer " scheme); the scheme is
        // validated and stripped in authenticateToken, right before the Manage API client call.
        return $request->headers->get(self::MANAGE_TOKEN_HEADER)
            ?? $request->headers->get(self::SERVICE_ACCOUNT_HEADER);
    }

    public function authenticateToken(
        AuthAttributeInterface $authAttribute,
        string $token,
        Request $request,
    ): ApplicationToken {
        assert($authAttribute instanceof ApplicationTokenAuth);

        if ($request->headers->has(self::MANAGE_TOKEN_HEADER)) {
            if ($token === '') {
                throw new CustomUserMessageAuthenticationException(
                    sprintf('Invalid %s header: token must not be empty', self::MANAGE_TOKEN_HEADER),
                );
            }
            $manageApiClient = $this->manageApiClientFactory->getClientForManageToken($token);
        } else {
            $manageApiClient = $this->manageApiClientFactory->getClientForServiceAccountToken(
                $this->stripBearerScheme($token),
            );
        }

        try {
            $tokenData = $manageApiClient->verifyToken();
        } catch (ManageApiClientException $e) {
            throw new CustomUserMessageAuthenticationException($e->getMessage(), [], 0, $e);
        }

        return ApplicationToken::fromVerifyResponse($tokenData);
    }

    /**
     * The ServiceAccount JWT travels with the "Bearer " scheme; the Manage API client re-adds the
     * scheme, so it needs the bare token. A missing scheme means the header is malformed.
     */
    private function stripBearerScheme(string $headerValue): string
    {
        if (preg_match('/^Bearer\s+(.+)$/i', $headerValue, $matches) !== 1) {
            throw new CustomUserMessageAuthenticationException(
                sprintf('Invalid %s header: expected "Bearer <token>"', self::SERVICE_ACCOUNT_HEADER),
            );
        }

        return $matches[1];
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof ApplicationTokenAuth);
        assert($token instanceof ApplicationToken);

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
