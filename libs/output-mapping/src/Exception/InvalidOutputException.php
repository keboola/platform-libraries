<?php

namespace Keboola\OutputMapping\Exception;

class InvalidOutputException extends \RuntimeException
{
    public function __construct($message, $code = 0, $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
