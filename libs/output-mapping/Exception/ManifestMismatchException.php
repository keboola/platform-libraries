<?php

namespace Keboola\OutputMapping\Exception;

class ManifestMismatchException extends \RuntimeException
{
    public function __construct($message, $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
