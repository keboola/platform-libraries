<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

/**
 * Formats the numbers that go into the input mapping job log.
 *
 * Job logs are read by humans, so sizes are scaled to a binary unit. Durations are always printed in
 * seconds - the log line already carries a wall-clock timestamp, so a humanised "34 min" adds nothing
 * and only makes the value harder to compare across lines.
 */
final class LogFormatter
{
    private const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];

    public static function formatDuration(float $seconds): string
    {
        return sprintf('%.2f s', $seconds);
    }

    public static function formatBytes(int $bytes): string
    {
        $value = (float) $bytes;
        $unitIndex = 0;
        $lastUnitIndex = count(self::BYTE_UNITS) - 1;

        while ($value >= 1024.0 && $unitIndex < $lastUnitIndex) {
            $value /= 1024.0;
            $unitIndex++;
        }

        if ($unitIndex === 0) {
            return sprintf('%d B', $bytes);
        }

        return sprintf('%.1f %s', $value, self::BYTE_UNITS[$unitIndex]);
    }

    public static function formatThroughput(int $bytes, float $seconds): string
    {
        if ($seconds <= 0.0) {
            return 'n/a';
        }

        return self::formatBytes((int) round($bytes / $seconds)) . '/s';
    }
}
