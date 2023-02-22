<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use Keboola\AzureApiClient\ApiClient;

/**
 * @phpstan-import-type Options from AzureApiClientFactoryInterface
 */
class UnauthenticatedAzureApiClientFactory
{
    /**
     * @param Options $options
     */
    public function __construct(
        private readonly array $options = [],
    ) {
    }

    /**
     * @param non-empty-string $baseUrl
     * @param Options $options
     */
    public function createClient(string $baseUrl, array $options = []): ApiClient
    {
        $options = array_merge($this->options, $options);
        $options['baseUrl'] = $baseUrl;

        return new ApiClient($options);
    }
}
