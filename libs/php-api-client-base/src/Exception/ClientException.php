<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Exception;

use RuntimeException;
use Throwable;

class ClientException extends RuntimeException
{
    public function __construct(
        string $message = '',
        int $code = 0,
        ?Throwable $previous = null,
        private readonly ?int $statusCode = null,
        private readonly ?string $responseBody = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * HTTP status code of the failing response, or null when there was no response
     * (transport/connection/authenticator failure).
     */
    public function getStatusCode(): ?int
    {
        return $this->statusCode;
    }

    /**
     * Raw response body when available, otherwise null.
     */
    public function getResponseBody(): ?string
    {
        return $this->responseBody;
    }
}
