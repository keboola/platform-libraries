<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

class Path
{
    private const SEPARATOR = '/';

    /**
     * @param string ...$parts
     */
    public static function join(...$parts): string
    {
        if (count($parts) === 0) {
            return '';
        }

        // first part needs to be handled separately to not trim the leading slash of absolute paths
        $firstPart = array_shift($parts);
        $firstPart = self::noTrailingSlash($firstPart);

        if (count($parts) === 0) {
            return $firstPart;
        }

        $parts = array_map(function ($part) {
            return trim($part, self::SEPARATOR);
        }, $parts);

        array_unshift($parts, $firstPart);

        $parts = array_filter($parts, function ($part) {
            return $part !== '' && $part !== self::SEPARATOR;
        });

        $joined = implode(self::SEPARATOR, $parts);

        return self::noTrailingSlash($joined);
    }

    public static function ensureTrailingSlash(string $path): string
    {
        if ($path === '') {
            return $path;
        }

        return rtrim($path, self::SEPARATOR) . self::SEPARATOR;
    }

    public static function noTrailingSlash(string $path): string
    {
        if ($path === self::SEPARATOR) {
            return $path;
        }

        return rtrim($path, self::SEPARATOR);
    }
}
