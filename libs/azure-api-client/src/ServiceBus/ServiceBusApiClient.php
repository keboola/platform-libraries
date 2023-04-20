<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus;

use DateTimeImmutable;
use DateTimeZone;
use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\ServiceBus\Model\BrokerProperties;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusBrokerMessageRequest;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusBrokerMessageResponse;
use Keboola\AzureApiClient\Json;
use Webmozart\Assert\Assert;

class ServiceBusApiClient
{
    private const MAXIMUM_MESSAGE_DELAY_SECONDS = 900;
    private const NO_MESSAGE_HTTP_CODE = 204;
    private const POOLING_TIMEOUT_SECONDS = 10;
    public const AZURE_DATE_FORMAT = 'D, d M Y H:i:s T';

    private ApiClient $apiClient;

    public function __construct(
        string $serviceBusEndpoint,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient(
            $serviceBusEndpoint,
            $configuration,
        );
        $this->apiClient->authenticate(Resources::AZURE_SERVICE_BUS);
    }

    public function peakMessage(string $queueName): ?ServiceBusBrokerMessageResponse
    {
        $response = $this->apiClient->sendRequest(
            new Request(
                'POST',
                sprintf('/%s/head?timeout=%d', $queueName, self::POOLING_TIMEOUT_SECONDS),
            )
        );
        if ($response->getStatusCode() === self::NO_MESSAGE_HTTP_CODE) {
            return null;
        }

        $brokerProperties = BrokerProperties::fromResponseData(
            Json::decodeArray($response->getHeaderLine('BrokerProperties'))
        );
        $lockLocation = $response->getHeaderLine('Location');

        return new ServiceBusBrokerMessageResponse(
            $brokerProperties->messageId,
            $response->getBody()->getContents(),
            $lockLocation
        );
    }

    public function sendMessage(string $queueName, ServiceBusBrokerMessageRequest $message, int $delay = 0): void
    {
        $delaySeconds = (int) min($delay, self::MAXIMUM_MESSAGE_DELAY_SECONDS);
        $enqueueAt = new DateTimeImmutable(
            sprintf('+ %s seconds', $delaySeconds),
            new DateTimeZone('UTC')
        );
        $this->apiClient->sendRequest(
            new Request(
                'POST',
                sprintf('/%s/messages', $queueName),
                [
                    'Content-Type' => $message->contentType,
                    'BrokerProperties' => json_encode([
                        'MessageId' => $message->id,
                        'ScheduledEnqueueTimeUtc' => $enqueueAt->format(self::AZURE_DATE_FORMAT),
                    ], JSON_THROW_ON_ERROR),
                ],
                $message->body
            ),
        );
    }

    public function deleteMessage(string $lockLocation): void
    {
        $lockLocationArray = parse_url($lockLocation);
        $lockLocationPath = '';

        if (array_key_exists('path', $lockLocationArray)) {
            $lockLocationPath = $lockLocationArray['path'];
            $lockLocationPath = preg_replace(
                '@^\/@',
                '',
                $lockLocationPath
            );
        }
        Assert::stringNotEmpty($lockLocationPath, 'Lock location path must be set.');

        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                $lockLocationPath,
            )
        );
    }
}
