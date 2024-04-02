<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper\Exception;

use Keboola\CommonExceptions\ExceptionWithContextInterface;
use Keboola\CommonExceptions\UserExceptionInterface;
use Throwable;

class InvalidPayloadException extends RequestMapperException implements
    UserExceptionInterface,
    ExceptionWithContextInterface
{
    public function __construct(
        string $message,
        int $code = 400,
        private readonly array $context = [],
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    public function getContext(): array
    {
        return $this->context;
    }
}
