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
     * @param ?Closure(ResponseInterface): ?bool $retryableResponse Decides whether a response is retried: true
     *     retries it, false does not, null leaves it to the status-code rules. It is asked for every response,
     *     including a successful one, but not for a transport error. The body is rewound after the call.
     */
    public function __construct(
        private readonly int $maxRetries,
        private readonly LoggerInterface $logger,
        private readonly array $retryableStatusCodes = [],
        private readonly ?Closure $retryableResponse = null,
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

        if ($response !== null) {
            $code = $response->getStatusCode();

            $retryable = $this->isRetryableResponse($response);
            if ($retryable !== null) {
                return $retryable && $this->logAndRetry($code, $error, $retries);
            }

            // Explicitly retryable codes (e.g. 429) win over the generic 4xx no-retry rule.
            if (in_array($code, $this->retryableStatusCodes, true)) {
                return $this->logAndRetry($code, $error, $retries);
            }

            if ($code >= 400 && $code < 500) {
                return false;
            }

            if ($code >= 500) {
                return $this->logAndRetry($code, $error, $retries);
            }
        }

        if ($error !== null) {
            return $this->logAndRetry(null, $error, $retries);
        }

        return false;
    }

    private function isRetryableResponse(ResponseInterface $response): ?bool
    {
        if ($this->retryableResponse === null) {
            return null;
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
