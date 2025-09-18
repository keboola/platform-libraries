<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Exception;

use Throwable;

class ResponseException extends ClientException
{
    private ?array $responseData;

    public function __construct(
        string $message,
        int $code,
        ?array $responseData,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
        $this->responseData = $responseData;
    }

    public function getResponseData(): ?array
    {
        return $this->responseData;
    }

    public function getErrorCode(): ?string
    {
        if ($this->responseData === null) {
            return null;
        }

        $errorCode = $this->responseData['context']['errorCode'] ?? null;

        if ($errorCode === null) {
            return null;
        }

        return (string) $errorCode;
    }

    public function isErrorCode(string $errorCode): bool
    {
        return $this->getErrorCode() === $errorCode;
    }
}
