<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Exception;

use Throwable;

interface RetryableExceptionInterface extends Throwable
{
    /**
     * Seconds the caller should wait before retrying, sent as the `Retry-After` header; null when unknown.
     */
    public function getRetryAfter(): ?int;
}
