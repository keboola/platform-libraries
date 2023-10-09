<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent;

interface EventInterface
{
    public static function getEventName(): string;
    public static function fromArray(array $data): self;
    public function toArray(): array;

    public function getId(): string;
}
