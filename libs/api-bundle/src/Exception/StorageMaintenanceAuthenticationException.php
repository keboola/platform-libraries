<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Exception;

use Symfony\Component\HttpFoundation\Response;
use Throwable;

class StorageMaintenanceAuthenticationException extends ContextualAuthenticationException implements
    RetryableExceptionInterface
{
    public const ERROR_CODE = 'storage.maintenance';

    public function __construct(
        string $message,
        ?Throwable $previous = null,
        private readonly ?int $retryAfter = null,
    ) {
        parent::__construct(
            $message,
            Response::HTTP_SERVICE_UNAVAILABLE,
            ['errorCode' => self::ERROR_CODE],
            $previous,
        );
    }

    public function getRetryAfter(): ?int
    {
        return $this->retryAfter !== null && $this->retryAfter > 0 ? $this->retryAfter : null;
    }
}
