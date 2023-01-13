<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace\Model;

use DateTimeImmutable;

class UsageEventResult
{
    public function __construct(
        public readonly ?string $usageEventId,
        public readonly ?UsageEventError $error,
        public readonly string $status,
        public readonly DateTimeImmutable $messageTime,
        public readonly string $resourceId,
        public readonly string $planId,
        public readonly string $dimension,
        public readonly float $quantity,
        public readonly DateTimeImmutable $effectiveStartTime,
    ) {
    }

    public static function fromResponseData(array $data): self
    {
        return new self(
            $data['usageEventId'] ?? null,
            ($data['error'] ?? null) ? UsageEventError::fromResponseData($data['error']) : null,
            $data['status'],
            new DateTimeImmutable($data['messageTime']),
            $data['resourceId'],
            $data['planId'],
            $data['dimension'],
            (float) $data['quantity'],
            new DateTimeImmutable($data['effectiveStartTime']),
        );
    }

    public function wasAccepted(): bool
    {
        return $this->status === 'Accepted';
    }

    public function isDuplicate(): bool
    {
        return $this->status === 'Duplicate';
    }
}
