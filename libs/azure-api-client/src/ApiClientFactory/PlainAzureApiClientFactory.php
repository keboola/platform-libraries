<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use Keboola\AzureApiClient\ApiClient;
use Psr\Log\LoggerInterface;

/**
 * @phpstan-type Options array{
 *     backoffMaxTries?: null|int<0, max>,
 *     middleware?: null|list<callable>,
 *     requestHandler?: null|callable,
 *     logger?: null|LoggerInterface,
 * }
 */
class PlainAzureApiClientFactory
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
