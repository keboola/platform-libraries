<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace\Model;

class ResolveSubscriptionResult
{
    public function __construct(
        public readonly string $id,
        public readonly string $subscriptionName,
        public readonly string $offerId,
        public readonly string $planId,
        public readonly Subscription $subscription,
        public readonly array $rawData,
    ) {
    }

    public static function fromResponseData(array $data): self
    {
        return new self(
            $data['id'],
            $data['subscriptionName'],
            $data['offerId'],
            $data['planId'],
            Subscription::fromResponseData($data['subscription']),
            $data,
        );
    }
}
