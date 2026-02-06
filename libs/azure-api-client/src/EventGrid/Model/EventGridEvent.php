<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\EventGrid\Model;

use DateTimeImmutable;

final class EventGridEvent
{
    /**
     * @param array<mixed> $data
     */
    public function __construct(
        public readonly string $id,
        public readonly string $subject,
        public readonly array $data,
        public readonly string $eventType
    ) {
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'subject' => $this->subject,
            'data' => $this->data,
            'eventType' => $this->eventType,
            'eventTime' => (new DateTimeImmutable('now'))->format('Y-m-d\TH:i:s\Z'),
            'dataVersion' => '1.0',
        ];
    }
}
