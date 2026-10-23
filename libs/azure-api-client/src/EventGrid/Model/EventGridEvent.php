<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\EventGrid\Model;

use DateTimeImmutable;
use DateTimeZone;
use Webmozart\Assert\Assert;

/**
 * Event in the Event Grid schema.
 * https://learn.microsoft.com/en-us/azure/event-grid/event-schema
 */
final class EventGridEvent
{
    private const DEFAULT_DATA_VERSION = '1.0';

    public readonly DateTimeImmutable $eventTime;

    /**
     * @param non-empty-string $id
     * @param non-empty-string $subject
     * @param non-empty-string $eventType
     * @param DateTimeImmutable|null $eventTime null for the time the event is created
     */
    public function __construct(
        public readonly string $id,
        public readonly string $subject,
        public readonly array $data,
        public readonly string $eventType,
        ?DateTimeImmutable $eventTime = null,
        public readonly string $dataVersion = self::DEFAULT_DATA_VERSION,
    ) {
        Assert::stringNotEmpty($this->id);
        Assert::stringNotEmpty($this->subject);
        Assert::stringNotEmpty($this->eventType);
        $this->eventTime = ($eventTime ?? new DateTimeImmutable())->setTimezone(new DateTimeZone('UTC'));
    }

    /**
     * @return array{id: string, subject: string, data: array, eventType: string, eventTime: string,
     *     dataVersion: string}
     */
    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'subject' => $this->subject,
            'data' => $this->data,
            'eventType' => $this->eventType,
            'eventTime' => $this->eventTime->format('Y-m-d\TH:i:s\Z'),
            'dataVersion' => $this->dataVersion,
        ];
    }
}
