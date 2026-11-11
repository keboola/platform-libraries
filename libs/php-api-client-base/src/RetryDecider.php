<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Closure;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Throwable;

class RetryDecider
{
    /**
     * @param list<int> $retryableStatusCodes Deprecated, use $retryableResponse. Non-5xx status codes that should
     *     also be retried (e.g. [429]).
     * @param (Closure(ResponseInterface): ?bool)|bool|null $retryableResponse Decides whether a failed response
     *     (status >= 400) is retried: true retries it, false does not, null leaves it to the status-code rules.
     *     A bool is a shortcut for a callback that always returns it. The body is rewound after the callback.
     */
    public function __construct(
        private readonly int $maxRetries,
        private readonly LoggerInterface $logger,
        private readonly array $retryableStatusCodes = [],
        private readonly Closure|bool|null $retryableResponse = null,
    ) {
    }

    public function __invoke(
        int $retries,
        RequestInterface $request,
        ?ResponseInterface $response = null,
        mixed $error = null,
    ): bool {
        if ($retries >= $this->maxRetries) {
            return false;
        }

        $code = $response?->getStatusCode();

        if ($response !== null && $code >= 400) {
            $retryable = $this->isRetryableResponse($response);
            if ($retryable !== null) {
                return $retryable ? $this->logAndRetry($code, $error, $retries) : false;
            }
        }

        // Explicitly retryable codes (e.g. 429) win over the generic 4xx no-retry rule.
        if ($code !== null && in_array($code, $this->retryableStatusCodes, true)) {
            return $this->logAndRetry($code, $error, $retries);
        }

        if ($code !== null && $code >= 400 && $code < 500) {
            return false;
        }

        if ($error !== null || ($code !== null && $code >= 500)) {
            return $this->logAndRetry($code, $error, $retries);
        }

        return false;
    }

    private function isRetryableResponse(ResponseInterface $response): ?bool
    {
        if (!$this->retryableResponse instanceof Closure) {
            return $this->retryableResponse;
        }

        try {
            return ($this->retryableResponse)($response);
        } finally {
            if ($response->getBody()->isSeekable()) {
                $response->getBody()->rewind();
            }
        }
    }

    private function logAndRetry(?int $code, mixed $error, int $retries): bool
    {
        $this->logger->warning(sprintf(
            'Request failed (%s), retrying (%s of %s)',
            match (true) {
                $error instanceof Throwable => $error->getMessage(),
                $code !== null => 'HTTP ' . $code,
                default => 'unknown',
            },
            $retries + 1,
            $this->maxRetries,
        ));

        return true;
    }
}
