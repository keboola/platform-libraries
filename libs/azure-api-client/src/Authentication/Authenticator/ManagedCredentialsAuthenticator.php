<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\Model\TokenResponse;

class ManagedCredentialsAuthenticator implements AuthenticatorInterface
{
    private const INSTANCE_METADATA_SERVICE_ENDPOINT = 'http://169.254.169.254/';
    private const API_VERSION = '2019-11-01';

    private ApiClient $apiClient;

    public function __construct(
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient(
            self::INSTANCE_METADATA_SERVICE_ENDPOINT,
            $configuration,
        );
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
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
}
