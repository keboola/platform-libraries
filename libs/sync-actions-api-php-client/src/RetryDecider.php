<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Throwable;

class RetryDecider
{
    public function __construct(
        /** @var positive-int $maxRetries */
        private readonly int $maxRetries,
        private readonly LoggerInterface $logger,
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

        $code = null;
        if ($response) {
            $code = $response->getStatusCode();
        } elseif ($error && $error instanceof Throwable) {
            $code = $error->getCode();
        }

        if ($code >= 400 && $code < 500) {
            return false;
        }

        if ($error || $code >= 500) {
            $this->logger->warning(
                sprintf(
                    'Request failed (%s), retrying (%s of %s)',
                    match (true) {
                        $error instanceof Throwable => $error->getMessage(),
                        is_scalar($error) => $error,
                        $response !== null => $response->getBody()->getContents(),
                        default => 'No error',
                    },
                    $retries,
                    $this->maxRetries,
                ),
            );
            return true;
        }

        return false;
    }
}
