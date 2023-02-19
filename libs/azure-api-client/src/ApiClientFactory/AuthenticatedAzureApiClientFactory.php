<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use GuzzleHttp\Middleware;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;

/**
 * @phpstan-import-type Options from PlainAzureApiClientFactory
 */
class AuthenticatedAzureApiClientFactory
{
    /**
     * @param Options $options
     */
    public function __construct(
        private readonly AuthenticatorFactory $authenticatorFactory,
        private readonly array $options = [],
    ) {
    }

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $resource
     * @param Options          $options
     */
    public function createClient(string $baseUrl, string $resource, array $options = []): ApiClient
    {
        $options = array_merge($this->options, $options);
        $options['baseUrl'] = $baseUrl;

        $options['middleware'] ??= [];
        $options['middleware'][] = Middleware::mapRequest(new AuthorizationHeaderResolver(
            $this->authenticatorFactory,
            $resource
        ));

        return new ApiClient($options);
    }
}
