<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientFactory\AuthorizationHeaderResolverInterface;
use Keboola\AzureApiClient\ApiClientFactory\BearerAuthorizationHeaderResolver;
use Keboola\AzureApiClient\ApiClientFactory\UnauthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Log\LoggerInterface;

class ClientCredentialsEnvironmentAuthenticator implements AuthenticatorInterface
{
    private const ENV_AZURE_AD_RESOURCE = 'AZURE_AD_RESOURCE';
    private const ENV_AZURE_ENVIRONMENT = 'AZURE_ENVIRONMENT';
    private const ENV_AZURE_TENANT_ID = 'AZURE_TENANT_ID';
    private const ENV_AZURE_CLIENT_ID = 'AZURE_CLIENT_ID';
    private const ENV_AZURE_CLIENT_SECRET = 'AZURE_CLIENT_SECRET';
    private const DEFAULT_ARM_URL = 'https://management.azure.com/metadata/endpoints?api-version=2020-01-01';
    private const DEFAULT_PUBLIC_CLOUD_NAME = 'AzureCloud';

    private ApiClient $apiClient;

    private string $tenantId;

    private string $clientId;

    private string $clientSecret;

    private string $armUrl;

    private string $cloudName;

    private ?string $authEndpoint = null;

    public function __construct(
        UnauthenticatedAzureApiClientFactory $clientFactory,
        private readonly LoggerInterface $logger,
    ) {
        $this->armUrl = (string) getenv(self::ENV_AZURE_AD_RESOURCE);
        if (!$this->armUrl) {
            $this->armUrl = self::DEFAULT_ARM_URL;
            $this->logger->debug(
                self::ENV_AZURE_AD_RESOURCE . ' environment variable is not specified, falling back to default.'
            );
        }

        $this->cloudName = (string) getenv(self::ENV_AZURE_ENVIRONMENT);
        if (!$this->cloudName) {
            $this->cloudName = self::DEFAULT_PUBLIC_CLOUD_NAME;
            $this->logger->debug(
                self::ENV_AZURE_ENVIRONMENT . ' environment variable is not specified, falling back to default.'
            );
        }

        $this->tenantId = (string) getenv(self::ENV_AZURE_TENANT_ID);
        $this->clientId = (string) getenv(self::ENV_AZURE_CLIENT_ID);
        $this->clientSecret = (string) getenv(self::ENV_AZURE_CLIENT_SECRET);
        $this->apiClient = $clientFactory->createClient($this->armUrl);
    }

    public function getAuthenticationToken(string $resource): TokenWithExpiration
    {
        if ($this->authEndpoint === null) {
            $this->authEndpoint = $this->getMetadata($this->armUrl, $this->cloudName)->authenticationLoginEndpoint;
        }

        $request = new Request('POST', sprintf('%s%s/oauth2/token', $this->authEndpoint, $this->tenantId));
        $formData = [
            'grant_type' => 'client_credentials',
            'client_id' => $this->clientId,
            'client_secret' => $this->clientSecret,
            'resource' => $resource,
        ];

        $token = $this->apiClient->sendRequestAndMapResponse(
            $request,
            TokenWithExpiration::class,
            ['form_params' => $formData]
        );

        $this->logger->info('Successfully authenticated using client credentials.');
        return $token;
    }

    public function checkUsability(): void
    {
        $errors = [];
        foreach ([self::ENV_AZURE_TENANT_ID, self::ENV_AZURE_CLIENT_ID, self::ENV_AZURE_CLIENT_SECRET] as $envVar) {
            if (!getenv($envVar)) {
                $errors[] = sprintf('Environment variable "%s" is not set.', $envVar);
            }
        }
        if ($errors) {
            throw new ClientException(implode(' ', $errors));
        }
    }

    private function getMetadata(string $armUrl, string $cloudName): MetadataResponse
    {
        try {
            $metadataList = $this->apiClient->sendRequestAndMapResponse(
                new Request('GET', $armUrl),
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

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface
    {
        return new BearerAuthorizationHeaderResolver($this, $resource);
    }
}
