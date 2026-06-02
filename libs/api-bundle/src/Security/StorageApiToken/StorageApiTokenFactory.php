<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

/**
 * Builds a {@see StorageApiToken} by verifying a token against Storage API. Shared by the
 * legacy {@see StorageApiTokenAuthenticator} flow and the programmatic-token exchange flow.
 */
class StorageApiTokenFactory
{
    public function __construct(
        private readonly StorageClientRequestFactory $clientRequestFactory,
    ) {
    }

    /**
     * Verifies the token carried by the request (Authorization: Bearer or X-StorageApi-Token).
     */
    public function createFromRequest(Request $request): StorageApiToken
    {
        try {
            $wrapper = $this->clientRequestFactory->createClientWrapper($request);
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

    /**
     * Verifies a legacy Storage token resolved from a programmatic token. The resolved token is
     * placed on a copy of the request as {@see StorageClientRequestFactory::TOKEN_HEADER} and any
     * incoming Authorization header is dropped, so the Storage client uses the legacy token (not
     * the original bearer token). The original request is left untouched.
     */
    public function createFromResolvedToken(
        Request $request,
        #[SensitiveParameter]
        string $legacyStorageToken,
    ): StorageApiToken {
        $exchangedRequest = clone $request;
        $exchangedRequest->headers = clone $request->headers;
        $exchangedRequest->headers->remove('Authorization');
        $exchangedRequest->headers->set(StorageClientRequestFactory::TOKEN_HEADER, $legacyStorageToken);

        return $this->createFromRequest($exchangedRequest);
    }
}
