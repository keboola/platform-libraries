<?php

namespace Keboola\OutputMapping\Exception;

class InvalidOutputException extends \RuntimeException
{
    public function __construct($message, $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
