<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
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
        assert($authAttribute instanceof StorageApiTokenAuth);

        try {
            if (!$this->requestStack->getMainRequest()) {
                throw new AccessDeniedException('No main request');
            }
            $wrapper = $this->clientRequestFactory->createClientWrapper($this->requestStack->getMainRequest());
            $storageApiClient = $wrapper->getBasicClient();
            $tokenInfo = $storageApiClient->verifyToken();
        } catch (ClientException $e) {
            if ($e->getCode() >= 400 && $e->getCode() < 500) {
                throw new CustomUserMessageAuthenticationException($e->getMessage(), [], $e->getCode(), $e);
            }
            throw $e;
        }

        return new StorageApiToken($tokenInfo, $storageApiClient->getTokenString());
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
