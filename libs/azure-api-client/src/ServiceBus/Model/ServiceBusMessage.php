<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ServiceBus\Model;

use DateTimeImmutable;
use Keboola\AzureApiClient\Json;
use Webmozart\Assert\Assert;

final class ServiceBusMessage
{
    /**
     * @param non-empty-string $id
     * @param non-empty-string $contentType
     * @param DateTimeImmutable|null $scheduledEnqueueTime When the message becomes visible to receivers,
     *     null to enqueue it immediately.
     */
    public function __construct(
        public readonly string $id,
        public readonly string $body,
        public readonly string $contentType,
        public readonly ?DateTimeImmutable $scheduledEnqueueTime = null,
    ) {
        Assert::stringNotEmpty($this->id);
        Assert::stringNotEmpty($this->contentType);
    }

    /**
     * @param non-empty-string $id
     */
    public static function json(string $id, array $data, ?DateTimeImmutable $scheduledEnqueueTime = null): self
    {
        return new self($id, Json::encodeArray($data), 'application/json', $scheduledEnqueueTime);
    }
}
