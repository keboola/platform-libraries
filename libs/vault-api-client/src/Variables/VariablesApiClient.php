<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Variables;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\VaultApiClient\Exception\VaultClientException;
use Keboola\VaultApiClient\Variables\Model\ListOptions;
use Keboola\VaultApiClient\Variables\Model\Variable;
use Keboola\VaultApiClient\VaultErrorMessageResolver;
use Psr\Log\LoggerInterface;
use Webmozart\Assert\Assert;

class VariablesApiClient
{
    private const FALLBACK_USER_AGENT = 'Keboola Vault PHP Client';
    private const DEFAULT_BACKOFF_MAX_TRIES = 10;

    private ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $token
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        string $baseUrl,
        string $token,
        ?LoggerInterface $logger = null,
        int $backoffMaxTries = self::DEFAULT_BACKOFF_MAX_TRIES,
        int $connectTimeout = ApiClientOptions::DEFAULT_CONNECT_TIMEOUT,
        int $requestTimeout = ApiClientOptions::DEFAULT_REQUEST_TIMEOUT,
        string $userAgent = self::FALLBACK_USER_AGENT,
        null|Closure|HandlerStack $requestHandler = null,
    ) {
        Assert::stringNotEmpty($baseUrl, 'Base URL must be a non-empty string');

        $this->apiClient = new ApiClient(
            $baseUrl,
            new StorageApiTokenAuthenticator($token),
            new ApiClientOptions(
                userAgent: $userAgent,
                backoffMaxTries: $backoffMaxTries,
                connectTimeout: $connectTimeout,
                requestTimeout: $requestTimeout,
                requestHandler: $requestHandler,
                logger: $logger,
            ),
            errorMessageResolver: new VaultErrorMessageResolver(),
            exceptionClass: VaultClientException::class,
        );
    }

    /**
     * @param non-empty-string $key
     * @param array<Variable::FLAG_*> $flags
     */
    public function createVariable(
        string $key,
        string $value,
        array $flags = [],
        array $attributes = [],
    ): Variable {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'variables',
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    'key' => $key,
                    'value' => $value,
                    'flags' => $flags,
                    'attributes' => $attributes,
                ]),
            ),
            Variable::class,
        );
    }

    /**
     * @param non-empty-string $hash
     */
    public function deleteVariable(string $hash): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                sprintf('variables/%s', $hash),
            ),
        );
    }

    /**
     * @return array<Variable>
     */
    public function listVariables(ListOptions $options): array
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                'variables?' . $options->asQueryString(),
            ),
            Variable::class,
            [],
            true,
        );
    }

    /**
     * @param non-empty-string $branchId
     * @return array<Variable>
     */
    public function listScopedVariablesForBranch(string $branchId): array
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                'variables/scoped/branch/' . $branchId,
            ),
            Variable::class,
            [],
            true,
        );
    }
}
