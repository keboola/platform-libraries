<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\EventGrid;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientFactory\AuthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;

class EventGridApiClient
{
    private const API_VERSION = '2018-01-01';

    public function __construct(
        private readonly ApiClient $azureApiClient,
    ) {
    }

    public static function create(
        AuthenticatorInterface $authenticator,
        string $topicHostname
    ): self {
        $apiClient = (new AuthenticatedAzureApiClientFactory($authenticator))->createClient(
            sprintf('https://%s', $topicHostname),
            Resources::AZURE_EVENT_GRID
        );
        return new self($apiClient);
    }

    /**
     * @param EventGridEvent[] $events
     */
    public function publishEvents(array $events): void
    {
        $this->azureApiClient->sendRequest(
            new Request(
                'POST',
                sprintf('/api/events?api-version=%s', self::API_VERSION),
                [],
                (string) json_encode(array_map(
                    static fn($event) => $event->toArray(),
                    $events
                ), JSON_THROW_ON_ERROR),
            ),
        );
    }
}
