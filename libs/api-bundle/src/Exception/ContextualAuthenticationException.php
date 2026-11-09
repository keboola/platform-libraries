<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Exception;

use Keboola\CommonExceptions\ExceptionWithContextInterface;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;
use Throwable;

class ContextualAuthenticationException extends CustomUserMessageAuthenticationException implements
    ExceptionWithContextInterface
{
    /**
     * @param array<mixed> $context
     */
    public function __construct(
        string $message,
        int $code,
        private readonly array $context,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, [], $code, $previous);
    }

    public function getContext(): array
    {
        return $this->context;
    }
}
