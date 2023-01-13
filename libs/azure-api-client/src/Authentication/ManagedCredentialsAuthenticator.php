<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use GuzzleHttp\Exception\GuzzleException;
use JsonException;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Psr\Log\LoggerInterface;

class ManagedCredentialsAuthenticator implements AuthenticatorInterface
{
    private GuzzleClientFactory $clientFactory;
    private LoggerInterface $logger;
    private ?string $cachedToken;

    private const INSTANCE_METADATA_SERVICE_ENDPOINT = 'http://169.254.169.254/';
    private const API_VERSION = '2019-11-01';

    public function __construct(GuzzleClientFactory $clientFactory)
    {
        $this->logger = $clientFactory->getLogger();
        $this->clientFactory = $clientFactory;
    }

    public function getAuthenticationToken(string $resource): string
    {
        if (!empty($this->cachedToken)) {
            return $this->cachedToken;
        }
        try {
            $client = $this->clientFactory->getClient(self::INSTANCE_METADATA_SERVICE_ENDPOINT);
            $response = $client->get(
                sprintf(
                    '/metadata/identity/oauth2/token?api-version=%s&format=text&resource=%s',
                    self::API_VERSION,
                    $resource
                ),
                [
                    'headers' => [
                        'Metadata' => 'true',
                    ],
                ]
            );
            $data = (array) json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
            if (empty($data['access_token']) || !is_scalar($data['access_token'])) {
                throw new InvalidResponseException('Access token not provided in response: ' . json_encode($data));
            }
            $this->logger->info('Successfully authenticated using instance metadata.');
            $this->cachedToken = (string) $data['access_token'];
            return $this->cachedToken;
        } catch (JsonException | GuzzleException $e) {
            throw new ClientException('Failed to get authentication token: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    public function checkUsability(): void
    {
        try {
            $client = $this->clientFactory->getClient(
                self::INSTANCE_METADATA_SERVICE_ENDPOINT,
                ['backoffMaxTries' => 1]
            );
            $client->get(
                sprintf('/metadata?api-version=%s&format=text', self::API_VERSION),
                [
                    'headers' => [
                        'Metadata' => 'true',
                    ],
                ]
            );
        } catch (GuzzleException $e) {
            throw new ClientException('Instance metadata service not available: ' . $e->getMessage(), 0, $e);
        }
    }
}
