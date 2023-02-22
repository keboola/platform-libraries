<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use Keboola\AzureApiClient\ApiClient;

/**
 * @phpstan-type Options array{
 *     backoffMaxTries?: null|int<0, max>,
 *     middleware?: null|list<callable>,
 *     requestHandler?: null|callable,
 *     logger?: null|LoggerInterface,
 * }
 */
interface AzureApiClientFactoryInterface
{
    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $resource
     * @param Options $options
     */
    public function createClient(string $baseUrl, string $resource, array $options = []): ApiClient;
}
