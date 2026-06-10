<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Symfony\Component\HttpClient\Retry\GenericRetryStrategy;

/**
 * Builds the {@see GenericRetryStrategy} used by {@see ApiClient}, encoding Keboola's
 * retry policy: retry transport errors and every 5xx, plus any service-declared extra
 * codes (e.g. 429).
 *
 * Symfony's default strategy only retries 5xx for idempotent methods. Keboola clients
 * historically retried all 5xx regardless of method (the Guzzle `RetryDecider`), so we
 * pass status codes as a flat list — `GenericRetryStrategy` then retries them for every
 * HTTP method.
 */
final class RetryStrategyFactory
{
    /**
     * @param list<int> $retryableStatusCodes Extra non-5xx codes to retry (e.g. [429]).
     */
    public static function create(array $retryableStatusCodes = []): GenericRetryStrategy
    {
        // 0 = retry on transport exceptions; flat 5xx list = retry for all methods.
        $statusCodes = [0, ...range(500, 599), ...$retryableStatusCodes];

        return new GenericRetryStrategy(
            statusCodes: array_values(array_unique($statusCodes)),
            delayMs: 1000,
            multiplier: 2.0,
            maxDelayMs: 0,
            jitter: 0.1,
        );
    }
}
