<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace\Model;

class ActivateSubscriptionRequest
{
    public function __construct(
        public readonly string $subscriptionId,
        public readonly string $planId,
        public readonly null|string|int|float $quantity = null,
    ) {
    }
}
