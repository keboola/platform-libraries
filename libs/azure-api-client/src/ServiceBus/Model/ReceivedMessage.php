<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus\Model;

final class ReceivedMessage
{
    /**
     * @param string $lockLocation URI of the lock, passed unchanged to ServiceBusApiClient::deleteMessage().
     */
    public function __construct(
        public readonly string $body,
        public readonly ?string $contentType,
        public readonly string $lockLocation,
        public readonly BrokerProperties $brokerProperties,
    ) {
    }
}
