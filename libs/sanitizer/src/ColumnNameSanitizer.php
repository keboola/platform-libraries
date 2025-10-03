<?php

declare(strict_types=1);

namespace Keboola\Utils\Sanitizer;

class ColumnNameSanitizer
{
    public const VALID_CHARACTERS = '_A-Za-z0-9';
    public const REPLACE_INVALID_CHARACTERS_WITH = '_';

    public static function sanitize(
        string $value,
        string $replaceCharactersWith = self::REPLACE_INVALID_CHARACTERS_WITH,
        bool $lower = false,
        ?int $maxLength = null,
    ): string {
        //@todo handle RS system columns ?
        return Filter::filter(
            $value,
            self::VALID_CHARACTERS,
            $replaceCharactersWith,
            $maxLength,
            $lower,
        );
    }
}
