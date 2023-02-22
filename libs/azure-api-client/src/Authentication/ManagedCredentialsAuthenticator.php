<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClientFactory\AuthorizationHeaderResolverInterface;
use Keboola\AzureApiClient\ApiClientFactory\BearerAuthorizationHeaderResolver;
use Keboola\AzureApiClient\ApiClientFactory\UnauthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Log\LoggerInterface;

class ManagedCredentialsAuthenticator implements AuthenticatorInterface
{
    private const INSTANCE_METADATA_SERVICE_ENDPOINT = 'http://169.254.169.254/';
    private const API_VERSION = '2019-11-01';

    public function __construct(
        private readonly UnauthenticatedAzureApiClientFactory $clientFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function getAuthenticationToken(string $resource): TokenWithExpiration
    {
        $client = $this->clientFactory->createClient(self::INSTANCE_METADATA_SERVICE_ENDPOINT);
        $token = $client->sendRequestAndMapResponse(
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
            TokenWithExpiration::class
        );

        $this->logger->info('Successfully authenticated using instance metadata.');
        return $token;
    }

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface
    {
        return new BearerAuthorizationHeaderResolver($this,$resource);
    }
}
