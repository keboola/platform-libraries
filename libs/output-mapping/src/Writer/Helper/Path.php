<?php

namespace Keboola\OutputMapping\Writer\Helper;

use InvalidArgumentException;

class Path
{
    const SEPARATOR = '/';

    /**
     * @param string ...$parts
     * @return string
     */
    public static function join(...$parts)
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

    /**
     * @param string $path
     * @return string
     */
    public static function ensureTrailingSlash($path)
    {
        if (!is_string($path)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $path must be a string, %s given',
                is_object($path) ? get_class($path) : gettype($path)
            ));
        }

        if ($path === '') {
            return $path;
        }

        return rtrim($path, self::SEPARATOR) . self::SEPARATOR;
    }

    /**
     * @param string $path
     * @return string
     */
    public static function noTrailingSlash($path)
    {
        if (!is_string($path)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $path must be a string, %s given',
                is_object($path) ? get_class($path) : gettype($path)
            ));
        }

        if ($path === self::SEPARATOR) {
            return $path;
        }

        return rtrim($path, self::SEPARATOR);
    }
}
