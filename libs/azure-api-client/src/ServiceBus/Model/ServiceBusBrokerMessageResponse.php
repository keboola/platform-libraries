<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus\Model;

use Keboola\AzureApiClient\Json;

final class ServiceBusBrokerMessageResponse
{
    public function __construct(
        public readonly string $id,
        public readonly string $body,
        public readonly string $lockLocation
    ) {
    }

    public function getJsonBody(): array
    {
        $body = Json::decodeArray($this->body);
        if (array_key_exists('objectId', $body)) {
            // Messages is missing wrapping in data object
            $body = ['data' => $body];
        }
        if (array_key_exists('topic', $body)) {
            // When EventGrid sends data to ServiceBus they are wrapped
            // in object with id,subject,data,eventType,dataVersion,metadataVersion,eventTime,topic props
            $body = $body['data'];
        }
        return $body;
    }
}
