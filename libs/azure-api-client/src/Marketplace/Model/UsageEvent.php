<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace\Model;

use DateTimeImmutable;
use JsonSerializable;

class UsageEvent implements JsonSerializable
{
    public function __construct(
        public readonly string $resourceId,
        public readonly string $planId,
        public readonly string $dimension,
        public readonly float $quantity,
        public readonly DateTimeImmutable $effectiveStartTime,
    ) {
    }

    public function jsonSerialize(): array
    {
        return [
            'resourceId' => $this->resourceId,
            'planId' => $this->planId,
            'dimension' => $this->dimension,
            'quantity' => $this->quantity,
            'effectiveStartTime' => $this->effectiveStartTime->format(DATE_ATOM),
        ];
    }
}
