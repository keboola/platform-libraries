<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Exception;

use RuntimeException;
use Throwable;

class PrimaryKeyNotChangedException extends RuntimeException
{
    public function __construct(string $message, int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
