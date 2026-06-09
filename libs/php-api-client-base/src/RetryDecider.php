<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Throwable;

class RetryDecider
{
    /**
     * @param list<int> $retryableStatusCodes Non-5xx status codes that should also be retried (e.g. [429]).
     */
    public function __construct(
        private readonly int $maxRetries,
        private readonly LoggerInterface $logger,
        private readonly array $retryableStatusCodes = [],
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

    private function logAndRetry(?int $code, mixed $error, int $retries): bool
    {
        $this->logger->warning(sprintf(
            'Request failed (%s), retrying (%s of %s)',
            match (true) {
                $error instanceof Throwable => $error->getMessage(),
                $code !== null => 'HTTP ' . $code,
                default => 'unknown',
            },
            $retries,
            $this->maxRetries,
        ));

        return true;
    }
}
