<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Throwable;

class RetryDecider
{
    public function __construct(
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
        if ($response !== null) {
            $code = $response->getStatusCode();
        } elseif ($error instanceof Throwable) {
            $code = $error->getCode();
        }

        if ($code !== null && $code >= 400 && $code < 500) {
            return false;
        }

        if ($error !== null || ($code !== null && $code >= 500)) {
            $this->logger->warning(sprintf(
                'Request failed (%s), retrying (%s of %s)',
                match (true) {
                    $error instanceof Throwable => $error->getMessage(),
                    $response !== null => 'HTTP ' . $code,
                    default => 'unknown',
                },
                $retries,
                $this->maxRetries,
            ));
            return true;
        }

        return false;
    }
}
