<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace\Model;

class Subscription
{
    public function __construct(
        public readonly string $id,
        public readonly string $publisherId,
        public readonly string $offerId,
        public readonly string $planId,
        public readonly string $name,
        public readonly ?int $quantity,
        public readonly string $saasSubscriptionStatus,
    ) {
    }

    public static function fromResponseData(array $data): self
    {
        return new self(
            $data['id'],
            $data['publisherId'],
            $data['offerId'],
            $data['planId'],
            $data['name'],
            $data['quantity'] ?? null,
            $data['saasSubscriptionStatus'],
        );
    }
}
