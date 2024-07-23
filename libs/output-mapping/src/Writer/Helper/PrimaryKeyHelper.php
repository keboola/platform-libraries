<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Psr\Log\LoggerInterface;

class PrimaryKeyHelper
{
    /**
     * @param array $keys
     * @param LoggerInterface $logger
     * @return array
     */
    public static function normalizeKeyArray(LoggerInterface $logger, array $keys)
    {
        return array_map(
            function ($key) {
                return trim($key);
            },
            array_unique(
                array_filter($keys, function ($col) use ($logger) {
                    if ($col !== '') {
                        return true;
                    }
                    $logger->warning('Found empty column name in key array.');
                    return false;
                }),
            ),
        );
    }
}
