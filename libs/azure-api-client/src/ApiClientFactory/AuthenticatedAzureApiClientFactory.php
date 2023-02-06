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
    private ?AuthenticatorInterface $authenticator = null;

    public function __construct(
        private readonly GuzzleClientFactory $guzzleClientFactory,
        private readonly AuthenticatorFactory $authenticatorFactory,
    ) {
    }

    public function createClient(string $baseUrl, string $resource, array $options = []): ApiClient
    {
        $options['middleware'] ??= [];
        $options['middleware'][] = Middleware::mapRequest(function (RequestInterface $request) use ($resource) {
            static $token = null;
            if ($token === null) {
                $token = $this->getAuthenticationToken($resource);
            }

            return $request->withHeader('Authorization', 'Bearer ' . $token);
        });

        $guzzleClient = $this->guzzleClientFactory->getClient($baseUrl, $options);
        return new ApiClient($guzzleClient);
    }

    private function getAuthenticationToken(string $resource): string
    {
        $this->authenticator ??= $this->authenticatorFactory->createAuthenticator();
        return $this->authenticator->getAuthenticationToken($resource);
    }
}
