<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\ManageTokenAuth;
use Keboola\ApiBundle\Attribute\StorageTokenAuth;
use Keboola\ApiBundle\Security\ManageToken\ManageToken;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use Symfony\Component\HttpFoundation\RequestStack;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * @implements TokenAuthenticatorInterface<StorageApiToken>
 */
class StorageApiTokenAuthenticator implements TokenAuthenticatorInterface
{
    public function __construct(
        private readonly StorageClientRequestFactory $clientRequestFactory,
        private readonly RequestStack $requestStack,
    ) {
    }

    public function getTokenHeader(): string
    {
        return 'X-StorageApi-Token';
    }

    public function authenticateToken(AuthAttributeInterface $authAttribute, string $token): StorageApiToken
    {
        assert($authAttribute instanceof StorageTokenAuth);

        try {
            $wrapper = $this->clientRequestFactory->createClientWrapper($this->requestStack->getMainRequest());
            $storageApiClient = $wrapper->getBasicClient();
            $tokenInfo = $storageApiClient->verifyToken();
        } catch (ClientException $e) {
            throw new CustomUserMessageAuthenticationException($e->getMessage(), [], 0, $e);
        }

        return new StorageApiToken($tokenInfo, $storageApiClient->getTokenString());
    }

    public function authorizeToken(AuthAttributeInterface $authAttribute, TokenInterface $token): void
    {
        assert($authAttribute instanceof StorageTokenAuth);
        assert($token instanceof StorageApiToken);

        $missingFeatures = array_diff($authAttribute->features, $token->getFeatures());
        if (count($missingFeatures) > 0) {
            throw new AccessDeniedException(sprintf(
                'Authentication token is valid but missing following features: %s',
                implode(', ', $missingFeatures)
            ));
        }
    }
}
