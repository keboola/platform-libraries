<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClientFactory\PlainAzureApiClientFactory;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Log\LoggerInterface;

class ManagedCredentialsAuthenticator implements AuthenticatorInterface
{
    private ?string $cachedToken;

    private const INSTANCE_METADATA_SERVICE_ENDPOINT = 'http://169.254.169.254/';
    private const API_VERSION = '2019-11-01';

    public function __construct(
        private readonly PlainAzureApiClientFactory $clientFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function getAuthenticationToken(string $resource): string
    {
        if (!empty($this->cachedToken)) {
            return $this->cachedToken;
        }

        $client = $this->clientFactory->createClient(self::INSTANCE_METADATA_SERVICE_ENDPOINT);
        $token = $client->sendRequestAndMapResponse(
            new Request(
                'GET',
                sprintf(
                    '/metadata/identity/oauth2/token?%s',
                    http_build_query([
                        'api-version' => self::API_VERSION,
                        'format' => 'text',
                        'resource' => $resource,
                    ])
                ),
                [
                    'Metadata' => 'true',
                ],
            ),
            TokenResponse::class
        );

        $this->logger->info('Successfully authenticated using instance metadata.');
        return $this->cachedToken = $token->accessToken;
    }

    public function checkUsability(): void
    {
        try {
            $client = $this->clientFactory->createClient(
                self::INSTANCE_METADATA_SERVICE_ENDPOINT,
                ['backoffMaxTries' => 1]
            );
            $client->sendRequest(
                new Request(
                    'GET',
                    sprintf('/metadata?%s', http_build_query([
                        'api-version' => self::API_VERSION,
                        'format' => 'text',
                    ])),
                    [
                        'Metadata' => 'true',
                    ],
                ),
            );
        } catch (ClientException $e) {
            throw new ClientException('Instance metadata service not available: ' . $e->getMessage(), 0, $e);
        }
    }
}
