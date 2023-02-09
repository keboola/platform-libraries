<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Responses;

use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\Json;

class ArmMetadata
{
    private string $loginEndpoint;
    // @phpstan-ignore-next-line
    private string $keyVaultDns;
    // @phpstan-ignore-next-line
    private string $name;

    public function __construct(array $data)
    {
        if (!empty($data['name'])) {
            $this->name = (string) $data['name'];
        } else {
            throw new InvalidResponseException(
                '"name" field not found in API response: ' . Json::encodeArray($data)
            );
        }
        if (!empty($data['suffixes']['keyVaultDns'])) {
            $this->keyVaultDns = (string) $data['suffixes']['keyVaultDns'];
        } else {
            throw new InvalidResponseException(
                '"suffixes.keyVaultDns" field not found in API response: ' . Json::encodeArray($data)
            );
        }
        if (!empty($data['authentication']['loginEndpoint'])) {
            $this->loginEndpoint = (string) $data['authentication']['loginEndpoint'];
        } else {
            throw new InvalidResponseException(
                '"authentication.loginEndpoint" field not found in API response: ' . Json::encodeArray($data)
            );
        }
    }

    public function getAuthenticationLoginEndpoint(): string
    {
        return $this->loginEndpoint;
    }
}
