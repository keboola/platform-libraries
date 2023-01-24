<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Exception;

use RuntimeException;
use Throwable;

class OutputOperationException extends RuntimeException
{
    public function __construct(string $message, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
