<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus\Model;

final class ServiceBusBrokerMessageRequest
{
    public function __construct(
        public readonly string $id,
        public readonly string $body,
        public readonly string $contentType,
    ) {
    }

    public static function createJson(string $id, array $message): self
    {
        return new self(
            id: $id,
            body: json_encode($message, JSON_THROW_ON_ERROR),
            contentType: 'application/json',
        );
    }
}
