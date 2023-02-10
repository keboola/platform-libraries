<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use GuzzleHttp\Middleware;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Psr\Http\Message\RequestInterface;

class AuthenticatedAzureApiClientFactory
{
    public function __construct(
        private readonly GuzzleClientFactory $guzzleClientFactory,
        private readonly AuthenticatorFactory $authenticatorFactory,
    ) {
    }

    public function createClient(string $baseUrl, string $resource, array $options = []): ApiClient
    {
        $options['middleware'] ??= [];
        $options['middleware'][] = Middleware::mapRequest(new AuthorizationHeaderResolver(
            $this->authenticatorFactory,
            $resource
        ));

        $guzzleClient = $this->guzzleClientFactory->getClient($baseUrl, $options);
        return new ApiClient($guzzleClient);
    }
}
