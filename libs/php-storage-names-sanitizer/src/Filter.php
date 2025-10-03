<?php

declare(strict_types=1);

namespace Keboola\Utils\Sanitizer;

use Symfony\Component\String\UnicodeString;

class Filter
{
    public static function filter(
        string $value,
        string $validCharactersRegex,
        string $regexReplace,
        ?int $maxLength = null,
        bool $toLower = false,
    ): string {
        $returnString = (new UnicodeString($value))
            ->ascii()
            ->replaceMatches(
                '/[^' . $validCharactersRegex . ']+/u',
                $regexReplace,
            )
            ->trim($regexReplace)
            ->trimStart('_')
        ;

        if ($maxLength !== null) {
            $returnString = $returnString->truncate($maxLength);
        }

        if ($toLower) {
            $returnString = $returnString->lower();
        }

        return $returnString->toString();
    }
}
