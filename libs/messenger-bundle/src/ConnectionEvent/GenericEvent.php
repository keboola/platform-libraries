<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent;

class GenericEvent implements EventInterface
{
    public function __construct(
        private array $data,
    ) {
    }

    public static function getEventName(): string
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
