<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\Authenticator\Internal\BearerTokenResolver;
use Keboola\AzureApiClient\Authentication\Model\MetadataResponse;
use Keboola\AzureApiClient\Authentication\Model\TokenResponse;
use Keboola\AzureApiClient\Exception\ClientException;

class ClientCredentialsAuth implements BearerTokenResolver
{
    private const ENV_AZURE_AD_RESOURCE = 'AZURE_AD_RESOURCE';
    private const ENV_AZURE_ENVIRONMENT = 'AZURE_ENVIRONMENT';

    private const DEFAULT_ARM_URL = 'https://management.azure.com/metadata/endpoints?api-version=2020-01-01';
    private const DEFAULT_PUBLIC_CLOUD_NAME = 'AzureCloud';

    private ApiClient $apiClient;

    private string $cloudName;

    private ?string $authEndpoint = null;

    public function __construct(
        private readonly string $tenantId,
        private readonly string $clientId,
        private readonly string $clientSecret,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $configuration ??= new ApiClientConfiguration();

        $armUrl = (string) getenv(self::ENV_AZURE_AD_RESOURCE);
        if (!$armUrl) {
            $armUrl = self::DEFAULT_ARM_URL;
            $configuration->logger->debug(
                self::ENV_AZURE_AD_RESOURCE . ' environment variable is not specified, falling back to default.'
            );
        }

        $this->cloudName = (string) getenv(self::ENV_AZURE_ENVIRONMENT);
        if (!$this->cloudName) {
            $this->cloudName = self::DEFAULT_PUBLIC_CLOUD_NAME;
            $configuration->logger->debug(
                self::ENV_AZURE_ENVIRONMENT . ' environment variable is not specified, falling back to default.'
            );
        }

        $this->apiClient = new ApiClient($armUrl, $configuration);
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        $this->authEndpoint ??= $this->getMetadata($this->cloudName)->authenticationLoginEndpoint;

        $request = new Request('POST', sprintf('%s%s/oauth2/token', $this->authEndpoint, $this->tenantId));
        $formData = [
            'grant_type' => 'client_credentials',
            'client_id' => $this->clientId,
            'client_secret' => $this->clientSecret,
            'resource' => $resource,
        ];

        $token = $this->apiClient->sendRequestAndMapResponse(
            $request,
            TokenResponse::class,
            ['form_params' => $formData]
        );

        return new AuthenticationToken(
            $token->accessToken,
            $token->accessTokenExpiration,
        );
    }

    private function getMetadata(string $cloudName): MetadataResponse
    {
        try {
            $metadataList = $this->apiClient->sendRequestAndMapResponse(
                new Request('GET', ''),
                MetadataResponse::class,
                [],
                true
            );
        } catch (ClientException $e) {
            throw new ClientException('Failed to get instance metadata: ' . $e->getMessage(), $e->getCode(), $e);
        }

        foreach ($metadataList as $metadataItem) {
            if ($metadataItem->name === $cloudName) {
                return $metadataItem;
            }
        }

        throw new ClientException(sprintf('Cloud "%s" not found in instance metadata', $cloudName));
    }
}
