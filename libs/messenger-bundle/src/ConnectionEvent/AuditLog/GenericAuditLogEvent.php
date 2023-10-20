<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\AuditLog;

use Keboola\MessengerBundle\ConnectionEvent\EventInterface;

class GenericAuditLogEvent implements EventInterface
{
    public function __construct(
        private readonly array $data,
    ) {
    }

    public function getEventName(): string
    {
        return 'genericEvent';
    }

    public static function fromArray(array $data): self
    {
        return new self($data);
    }

    public function toArray(): array
    {
        return $this->data;
    }

    public function getId(): string
    {
        return (string) ($this->data['id'] ?? '');
    }

    public function getData(): array
    {
        return $this->data;
    }
}
