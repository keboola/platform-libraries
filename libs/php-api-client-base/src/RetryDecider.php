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
     * @param list<int> $retryableStatusCodes Non-5xx status codes that should also be retried (e.g. [429]).
     * @param ?Closure(ResponseInterface): bool $isNonRetryableResponse Returns true for a response that must
     *     not be retried, whatever its status code. The body is rewound after the call.
     */
    public function __construct(
        private readonly int $maxRetries,
        private readonly LoggerInterface $logger,
        private readonly array $retryableStatusCodes = [],
        private readonly ?Closure $isNonRetryableResponse = null,
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

        if ($response !== null && $this->isNonRetryableResponse($response)) {
            return false;
        }

        $code = $response?->getStatusCode();

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

    private function isNonRetryableResponse(ResponseInterface $response): bool
    {
        if ($this->isNonRetryableResponse === null) {
            return false;
        }

        try {
            return ($this->isNonRetryableResponse)($response);
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
