<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

interface ErrorMessageResolverInterface
{
    /**
     * Extract a human-readable error message from a failed response,
     * or return null to fall back to the default message.
     */
    public function __invoke(string $responseBody, int $statusCode): ?string;
}
