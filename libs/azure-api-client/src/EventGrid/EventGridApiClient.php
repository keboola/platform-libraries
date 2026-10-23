<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\EventGrid;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use Keboola\AzureApiClient\Json;
use Webmozart\Assert\Assert;

/**
 * Publishes events to an Event Grid topic.
 * https://learn.microsoft.com/en-us/rest/api/eventgrid/dataplane/publish-events
 */
class EventGridApiClient
{
    private const API_VERSION = '2018-01-01';

    private readonly ApiClient $apiClient;

    /**
     * @param string $topicHostname e.g. "my-topic.westeurope-1.eventgrid.azure.net"
     */
    public function __construct(
        string $topicHostname,
        ?ApiClientConfiguration $configuration = null,
    ) {
        if (filter_var($topicHostname, FILTER_VALIDATE_DOMAIN, FILTER_FLAG_HOSTNAME) === false
            || !str_contains($topicHostname, '.')
        ) {
            // the value is not echoed, an access key passed by mistake would end up in logs
            throw new InvalidArgumentException(
                'Event Grid topic hostname must be a bare hostname, e.g. "<topic>.<region>-1.eventgrid.azure.net"',
            );
        }

        $this->apiClient = new ApiClient(
            sprintf('https://%s/', $topicHostname),
            $configuration,
        );
        $this->apiClient->authenticate(Resources::AZURE_EVENT_GRID);
    }

    /**
     * @param non-empty-list<EventGridEvent> $events
     */
    public function publishEvents(array $events): void
    {
        Assert::notEmpty($events);
        Assert::allIsInstanceOf($events, EventGridEvent::class);

        $this->apiClient->sendRequest(new Request(
            'POST',
            sprintf('api/events?api-version=%s', self::API_VERSION),
            ['Content-Type' => 'application/json'],
            Json::encodeArray(array_map(
                static fn(EventGridEvent $event) => $event->toArray(),
                $events,
            )),
        ));
    }
}
