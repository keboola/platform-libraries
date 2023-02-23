<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolver;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolverInterface;
use Keboola\AzureApiClient\Authentication\Model\TokenResponse;
use Psr\Log\LoggerInterface;

class ManagedCredentialsAuthenticator implements AuthenticatorInterface
{
    private const INSTANCE_METADATA_SERVICE_ENDPOINT = 'http://169.254.169.254/';
    private const API_VERSION = '2019-11-01';

    private ApiClient $apiClient;

    public function __construct(
        null|callable $retryMiddleware = null,
        null|callable $requestHandler = null,
        null|LoggerInterface $logger = null,
    ) {
        $this->apiClient = new ApiClient(
            baseUrl: self::INSTANCE_METADATA_SERVICE_ENDPOINT,
            retryMiddleware: $retryMiddleware,
            requestHandler: $requestHandler,
            logger: $logger
        );
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        /** @var TokenResponse $token */
        $token = $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                sprintf(
                    '/metadata/identity/oauth2/token?%s',
                    http_build_query([
                        'api-version' => self::API_VERSION,
                        'format' => 'text',
                        'resource' => $resource,
                    ])
                ),
                [
                    'Metadata' => 'true',
                ],
            ),
            TokenResponse::class
        );

        return new AuthenticationToken(
            $token->accessToken,
            $token->accessTokenExpiration,
        );
    }

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface
    {
        return new AuthorizationHeaderResolver($this, $resource);
    }
}
