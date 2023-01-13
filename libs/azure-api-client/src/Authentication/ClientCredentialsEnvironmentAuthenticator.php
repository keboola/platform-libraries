<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Keboola\AzureApiClient\Responses\ArmMetadata;
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

    private Client $client;
    private LoggerInterface $logger;

    private string $tenantId;
    private string $clientId;
    private string $clientSecret;

    private string $armUrl;
    private string $cloudName;
    private ?string $cachedToken;

    public function __construct(GuzzleClientFactory $clientFactory)
    {
        $this->logger = $clientFactory->getLogger();
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
        $this->client = $clientFactory->getClient($this->armUrl);
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
            throw new ClientException(
                sprintf('Cloud "%s" not found in instance metadata: ' . json_encode($metadataArray), $cloudName)
            );
        }
        return new ArmMetadata($cloud);
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
            $response = $this->client->send($request);
            $metadata = json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($metadata)) {
                throw new InvalidResponseException(
                    'Invalid metadata contents: ' . json_encode($metadata)
                );
            }
            return $metadata;
        } catch (JsonException | GuzzleException $e) {
            throw new ClientException('Failed to get instance metadata: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    private function authenticate(string $authUrl, string $resource): string
    {
        try {
            $response = $this->client->post(
                sprintf('%s%s/oauth2/token', $authUrl, $this->tenantId),
                [
                    'form_params' => [
                        'grant_type' => 'client_credentials',
                        'client_id' => $this->clientId,
                        'client_secret' => $this->clientSecret,
                        'resource' => $resource,
                    ],
                    'headers' => [
                        'Content-type' => 'application/x-www-form-urlencoded',
                    ],
                ]
            );
            $data = (array) json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
            if (empty($data['access_token']) || !is_scalar($data['access_token'])) {
                throw new InvalidResponseException('Access token not provided in response: ' . json_encode($data));
            }
            $this->logger->info('Successfully authenticated using client credentials.');
            return (string) $data['access_token'];
        } catch (JsonException | GuzzleException $e) {
            throw new ClientException('Failed to get authentication token: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }
}
