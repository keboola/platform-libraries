<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\EventGrid;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use Keboola\AzureApiClient\Json;

class EventGridApiClient
{
    private const API_VERSION = '2018-01-01';

    private ApiClient $apiClient;

    public function __construct(
        string $topicHostname,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient(
            sprintf('https://%s', $topicHostname),
            $configuration,
        );
        $this->apiClient->authenticate(Resources::AZURE_EVENT_GRID);
    }

    /**
     * @param EventGridEvent[] $events
     */
    public function publishEvents(array $events): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'POST',
                sprintf('/api/events?api-version=%s', self::API_VERSION),
                [],
                Json::encodeArray(array_map(
                    static fn($event) => $event->toArray(),
                    $events
                )),
            ),
        );
    }
}
