<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientFactory\PlainAzureApiClientFactory;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\Responses\ArmMetadata;
use Keboola\AzureApiClient\Responses\ArrayDataResponse;
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
    private ?string $cachedToken;

    public function __construct(
        PlainAzureApiClientFactory $clientFactory,
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

    public function getAuthenticationToken(string $resource): string
    {
        if (empty($this->cachedToken)) {
            $metadata = $this->getMetadata($this->armUrl);
            $metadata = $this->processInstanceMetadata($metadata, $this->cloudName);
            $this->cachedToken = $this->authenticate($metadata->getAuthenticationLoginEndpoint(), $resource);
        }
        return $this->cachedToken;
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

    private function getMetadata(string $armUrl): array
    {
        try {
            $request = new Request('GET', $armUrl);
            return $this->apiClient->sendRequestAndMapResponse($request, ArrayDataResponse::class)->data;
        } catch (ClientException $e) {
            throw new ClientException('Failed to get instance metadata: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    private function processInstanceMetadata(array $metadataArray, string $cloudName): ArmMetadata
    {
        $cloud = null;
        foreach ($metadataArray as $item) {
            if (!empty($item['name']) && ($item['name'] === $cloudName)) {
                $cloud = $item;
            }
        }
        if (!$cloud) {
            throw new ClientException(sprintf(
                'Cloud "%s" not found in instance metadata: %s',
                $cloudName,
                json_encode($metadataArray)
            ));
        }
        return new ArmMetadata($cloud);
    }

    private function authenticate(string $authUrl, string $resource): string
    {
        $request = new Request('POST', sprintf('%s%s/oauth2/token', $authUrl, $this->tenantId));
        $formData = [
            'grant_type' => 'client_credentials',
            'client_id' => $this->clientId,
            'client_secret' => $this->clientSecret,
            'resource' => $resource,
        ];

        $data = $this->apiClient->sendRequestAndMapResponse(
            $request,
            ArrayDataResponse::class,
            ['form_params' => $formData]
        )->data;

        if (empty($data['access_token']) || !is_scalar($data['access_token'])) {
            throw new InvalidResponseException('Access token not provided in response: ' . json_encode($data));
        }

        $this->logger->info('Successfully authenticated using client credentials.');
        return (string) $data['access_token'];
    }
}
