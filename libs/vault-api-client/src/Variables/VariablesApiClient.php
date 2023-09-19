<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Variables;

use GuzzleHttp\Psr7\Request;
use Keboola\VaultApiClient\ApiClient;
use Keboola\VaultApiClient\ApiClientConfiguration;
use Keboola\VaultApiClient\Json;
use Keboola\VaultApiClient\Variables\Model\ListOptions;
use Keboola\VaultApiClient\Variables\Model\Variable;

class VariablesApiClient
{
    private ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $token
     */
    public function __construct(
        string $baseUrl,
        string $token,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient($baseUrl, $token, $configuration);
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
                ])
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
