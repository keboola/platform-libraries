<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use JsonException;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\ServiceBus\Exception\MessageLockLostException;
use Keboola\AzureApiClient\ServiceBus\Model\BrokerProperties;
use Keboola\AzureApiClient\ServiceBus\Model\ReceivedMessage;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusMessage;
use Throwable;
use Webmozart\Assert\Assert;

/**
 * Queue messaging over the Service Bus REST API: send, peek-lock receive and delete (complete).
 * https://learn.microsoft.com/en-us/rest/api/servicebus/service-bus-runtime-rest
 */
class ServiceBusApiClient
{
    private const HTTP_NO_CONTENT = 204;
    private const HTTP_NOT_FOUND = 404;
    private const HTTPS_PORT = 443;

    // queue, topic and subscription names: letters, digits, ".", "-", "_" and "/" separating path segments
    private const ENTITY_PATH_PATTERN = '~^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9_])?' .
        '(?:/[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9_])?)*\z~';

    private readonly ApiClient $apiClient;
    private readonly string $namespaceHost;
    private readonly int $requestTimeout;

    /**
     * @param string $namespaceEndpoint e.g. "https://my-namespace.servicebus.windows.net:443/"
     */
    public function __construct(
        string $namespaceEndpoint,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $configuration ??= new ApiClientConfiguration();

        $endpoint = parse_url($namespaceEndpoint);
        if (!is_array($endpoint)
            || strtolower($endpoint['scheme'] ?? '') !== 'https'
            || ($endpoint['host'] ?? '') === ''
        ) {
            // the value is not echoed, a connection string passed by mistake would put the key into logs
            throw new InvalidArgumentException(
                'Service Bus endpoint must be an https URL of the namespace, ' .
                'e.g. "https://<namespace>.servicebus.windows.net/"',
            );
        }
        $this->namespaceHost = strtolower($endpoint['host']);
        $this->requestTimeout = $configuration->requestTimeout;

        $this->apiClient = new ApiClient(
            sprintf('https://%s/', $this->namespaceHost),
            $configuration,
        );
        $this->apiClient->authenticate(Resources::AZURE_SERVICE_BUS);
    }

    /**
     * @throws InvalidArgumentException when the queue name is not a valid Service Bus entity path
     */
    public function sendMessage(string $queueName, ServiceBusMessage $message): void
    {
        $brokerProperties = ['MessageId' => $message->id];
        if ($message->scheduledEnqueueTime !== null) {
            $brokerProperties['ScheduledEnqueueTimeUtc'] = BrokerProperties::formatDate(
                $message->scheduledEnqueueTime,
            );
        }

        $this->apiClient->sendRequest(new Request(
            'POST',
            sprintf('%s/messages', self::entityPath($queueName)),
            [
                'Content-Type' => $message->contentType,
                'BrokerProperties' => Json::encodeArray($brokerProperties),
            ],
            $message->body,
        ));
    }

    /**
     * Receives the next message and locks it for the queue's lock duration. Delete it with deleteMessage()
     * once processed; if the lock expires first, the message is delivered again.
     *
     * @param positive-int $waitSeconds How long Service Bus holds the request open when the queue is empty.
     *     Must be shorter than the configured request timeout.
     * @return ReceivedMessage|null null when no message arrived within $waitSeconds
     */
    public function peekLockMessage(string $queueName, int $waitSeconds): ?ReceivedMessage
    {
        Assert::greaterThan($waitSeconds, 0);
        Assert::lessThan(
            $waitSeconds,
            $this->requestTimeout,
            'Wait time %s must be shorter than the request timeout %2$s, the request would time out first',
        );

        $response = $this->apiClient->sendRequest(new Request(
            'POST',
            sprintf('%s/messages/head?timeout=%d', self::entityPath($queueName), $waitSeconds),
        ));
        if ($response->getStatusCode() === self::HTTP_NO_CONTENT) {
            return null;
        }

        try {
            $brokerProperties = BrokerProperties::fromResponseData(
                Json::decodeArray($response->getHeaderLine('BrokerProperties')),
            );
        } catch (JsonException $e) {
            throw new InvalidResponseException('Response BrokerProperties header is not valid JSON', 0, $e);
        } catch (Throwable $e) {
            throw new InvalidResponseException('Invalid response BrokerProperties: ' . $e->getMessage(), 0, $e);
        }

        $lockLocation = $response->getHeaderLine('Location');
        if ($lockLocation === '') {
            throw new InvalidResponseException('Response is missing the Location header of the message lock');
        }

        $contentType = $response->getHeaderLine('Content-Type');

        return new ReceivedMessage(
            body: (string) $response->getBody(),
            contentType: $contentType === '' ? null : $contentType,
            lockLocation: $lockLocation,
            brokerProperties: $brokerProperties,
        );
    }

    /**
     * Completes a peek-locked message.
     *
     * @param string $lockLocation ReceivedMessage::$lockLocation
     * @throws MessageLockLostException when the lock expired or the message was already deleted
     * @throws InvalidArgumentException when the lock location does not point to this namespace
     */
    public function deleteMessage(string $lockLocation): void
    {
        // The request carries the namespace credentials, so it must never leave the namespace host.
        $lockUrl = parse_url($lockLocation);
        if (!is_array($lockUrl)
            || strtolower($lockUrl['scheme'] ?? '') !== 'https'
            || strtolower($lockUrl['host'] ?? '') !== $this->namespaceHost
            || ($lockUrl['port'] ?? self::HTTPS_PORT) !== self::HTTPS_PORT
            || isset($lockUrl['user'])
        ) {
            throw new InvalidArgumentException(sprintf(
                'Lock location "%s" does not belong to namespace "%s"',
                $lockLocation,
                $this->namespaceHost,
            ));
        }

        try {
            $this->apiClient->sendRequest(new Request('DELETE', $lockLocation));
        } catch (ClientException $e) {
            if ($e->getCode() === self::HTTP_NOT_FOUND) {
                throw new MessageLockLostException($e->getMessage(), $e->getCode(), $e);
            }
            throw $e;
        }
    }

    private static function entityPath(string $queueName): string
    {
        Assert::regex($queueName, self::ENTITY_PATH_PATTERN, 'Invalid Service Bus entity name "%s"');
        return $queueName;
    }
}
