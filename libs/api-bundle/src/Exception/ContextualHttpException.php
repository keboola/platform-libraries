<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Exception;

use Keboola\CommonExceptions\ExceptionWithContextInterface;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Throwable;

class ContextualHttpException extends HttpException implements ExceptionWithContextInterface
{
    /**
     * @param array<mixed> $context
     */
    public function __construct(
        int $statusCode,
        string $message,
        private readonly array $context,
        ?Throwable $previous = null,
    ) {
        parent::__construct($statusCode, $message, $previous);
    }

    public function getContext(): array
    {
        return $this->context;
    }
}
